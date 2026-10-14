def db_call(fn, *args, what="supabase call", **kwargs):
    for attempt in range(DB_RETRIES):
        try:
            return fn(*args, **kwargs)
        except httpx.TransportError as e:
            if attempt == DB_RETRIES - 1:
                log.error("%s failed after %d attempts: %s", what, DB_RETRIES, e)
                raise
            delay = 2 ** attempt
            log.warning(
                "%s failed (%s: %s) — retry %d/%d in %ss",
                what, type(e).__name__, e, attempt + 1, DB_RETRIES - 1, delay,
            )
            time.sleep(delay)


def claim_attempt(phone, counter_field, expected, stamp_field, now, extra=None):
    payload = {counter_field: expected + 1, stamp_field: now.strftime("%Y-%m-%d %H:%M")}
    if extra:
        payload.update(extra)
    q = supabase.table(SUPABASE_TABLE).update(payload).eq("phone", phone)
    if expected == 0:
        q = q.or_(f"{counter_field}.eq.0,{counter_field}.is.null")
    else:
        q = q.eq(counter_field, expected)
    return bool(db_call(q.execute, what="claim_attempt(%s)" % phone).data)


def _dial_and_capture(row, phone, caller_id, caller_index, call_track, dry_run, windows):
    captured = False
    if not dry_run:
        try:
            db_call(
                supabase.table(SUPABASE_TABLE).update(
                    {"voicemail_speech_detected": False}
                ).eq("phone", phone).execute,
                what="reset voicemail flag(%s)" % phone,
            )
        except Exception as e:
            log.warning("Failed to reset voicemail flag for %s: %s", phone, e)

    here = local_now(row, datetime.now(EASTERN))
    if not in_windows(windows, here):
        if not dry_run:
            STATS["closed_after_claim"] += 1
        log.warning(
            "%sWindow closed AFTER claiming the attempt for %s (%s) — %s local; "
            "not dialing, slot stays burned",
            "[DRY RUN] " if dry_run else "", row.get("clinic_name"), phone,
            here.strftime("%a %H:%M"),
        )
        return None, None, True

    result = place_call(phone, caller_id, caller_index, dry_run=dry_run)
    outcome = map_result(result["status"], result["answered_by"])

    if not dry_run:
        try:
            placed_at = datetime.now(EASTERN)
            db_call(
                supabase.table("audit_calls").upsert(
                    {
                        "call_sid": result.get("call_sid"),
                        "clinic_phone": phone,
                        "caller_number": caller_id,
                        "answered_by_raw": result.get("answered_by_raw"),
                        "duration_sec": (
                            int(result["duration"])
                            if result.get("duration") is not None else None
                        ),
                        "call_status": result.get("status"),
                        "outcome": outcome,
                        "call_track": call_track,
                        "after_hours": is_after_hours(local_now(row, placed_at)),
                        "batch": BATCH,
                        "called_at": placed_at.isoformat(),
                    },
                    on_conflict="call_sid",
                ).execute,
                what="capture audit_calls(%s)" % phone,
            )
            captured = True
        except Exception as e:
            STATS["capture_failed"] += 1
            log.error(
                "UNRECORDED CALL: audit_calls capture failed for %s (sid=%s) — "
                "call was placed and billed but will not appear in reporting: %s",
                phone, result.get("call_sid"), e,
            )

    # ... voicemail speech override omitted ...

    return outcome, result, (captured or dry_run)


def _run_main_call(row, phone, audit_type, main_cfg, now, dry_run):
    fresh = fetch_clinic(phone)
    if fresh is None:
        log.warning("Row for %s disappeared mid-run — skipping", phone)
        return False
    row.update(fresh)

    status = row.get("status", "pending")
    if status in ("done", "skip_ai"):
        return False

    if _within_cooldown(row.get("last_called"), main_cfg["cooldown_h"], now):
        log.info("Cooldown active for %s (%s) — skipping", row.get("clinic_name"), phone)
        return False

    max_attempts = main_cfg["max_attempts"]
    total_attempts = int(row.get("total_attempts") or 0)

    # ... attempt-cap and time-block rotation checks omitted ...

    used_indices = {
        int(x) for x in (row.get("callers_used") or "").split(",") if x.strip().isdigit()
    }
    caller_id, caller_index = pick_caller_id(used_indices)
    if caller_id is None:
        row["status"] = "done"
        if not dry_run:
            update_clinic(row)
        log.info(
            "%s%s (%s) [%s] exhausted the caller pool at %d attempts — marking done",
            "[DRY RUN] " if dry_run else "",
            row.get("clinic_name"), phone, audit_type, total_attempts,
        )
        return False

    now = datetime.now(EASTERN)
    here = local_now(row, now)
    if not in_windows(main_cfg["windows"], here):
        log.info(
            "Window closed while preparing %s (%s) — %s local; skipping",
            row.get("clinic_name"), phone, here.strftime("%a %H:%M"),
        )
        return False

    callers_used_after = ",".join(str(i) for i in sorted(used_indices | {caller_index}))

    if dry_run:
        log.info(
            "[DRY RUN] would claim attempt %d/%d for %s (%s) via caller-%d — no DB write",
            total_attempts + 1, max_attempts, row.get("clinic_name"), phone, caller_index,
        )
    elif not claim_attempt(
        phone, "total_attempts", total_attempts, "last_called", now,
        extra={"callers_used": callers_used_after},
    ):
        log.info(
            "Lost the race for %s (%s) — another dialer claimed attempt %d, skipping",
            row.get("clinic_name"), phone, total_attempts + 1,
        )
        return False

    log.info(
        "Calling %s (%s) [%s] attempt %d/%d via caller-%d — %s local (%s)",
        row.get("clinic_name"), phone, audit_type, total_attempts + 1, max_attempts,
        caller_index, here.strftime("%a %H:%M"), here.tzinfo,
    )

    outcome, result, captured = _dial_and_capture(
        row, phone, caller_id, caller_index, "weekday", dry_run, main_cfg["windows"]
    )

    total_attempts += 1
    row["total_attempts"] = total_attempts

    used_indices.add(caller_index)
    row["callers_used"] = callers_used_after
    row["last_called"] = now.strftime("%Y-%m-%d %H:%M")

    pool_exhausted = len(used_indices) >= len([p for p in CALLER_ID_POOL if "X" not in p])

    if outcome is None:
        if dry_run:
            return False
        row["status"] = "done" if (total_attempts >= max_attempts or pool_exhausted) else "in_progress"
        add_note(
            row, "attempt %d via caller-%d claimed but not dialed — window closed"
            % (total_attempts, caller_index),
        )
        _save_row_after_claim(row, phone, "Undialed-claim")
        return False

    STATS["simulated" if dry_run else "placed"] += 1

    existing_types = (row.get("answer_types") or "").strip()
    row["answer_types"] = (existing_types + "," + outcome).lstrip(",")

    all_types = row["answer_types"].split(",")
    row["picked_up_count"] = all_types.count("human")

    duration = result.get("duration")
    if outcome == "human" and duration in ("0", "1"):
        row["status"] = "skip_ai"
        add_note(row, "Possible AI receptionist — review before sending box")
        log.info("Flagged %s as possible AI receptionist", row.get("clinic_name"))
    elif total_attempts >= max_attempts or pool_exhausted:
        row["status"] = "done"
    else:
        row["status"] = "in_progress"

    if not captured:
        add_note(row, "call %s not captured in audit_calls" % result.get("call_sid"))

    if not dry_run:
        _save_row_after_claim(row, phone, "Post-call")

    log.info(
        "%sResult: %s | caller-%d | outcome=%s | callers_used=%s | status=%s",
        "[DRY RUN] " if dry_run else "",
        row.get("clinic_name"), caller_index, outcome, row["callers_used"], row["status"],
    )
    return True