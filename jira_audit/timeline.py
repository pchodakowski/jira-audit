from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
import sqlite3

from .business_time import BusinessCalendar
from .config import load_profile
from .db import get_connection


def parse_ts(ts_str: str) -> datetime:
    """
    Parse an ISO 8601 timestamp strng into a timezone-aware UTC datetime.
    Handles both 'Z' suffix and '+00:00' offset formats
    """
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def get_flagged_intervals(
        cur: sqlite3.Cursor,
        issue_key: str,
) -> list[tuple[datetime, Optional[datetime]]]:
    """
    Returns a list of (flagged_start, flagged_end) intervals for an issue.
    flagged_end is None if the issue is still flagged.
    """
    cur.execute(
        """
        SELECT changed_at, to_value
        FROM changelog_events
        WHERE issue_key = ? AND field = 'Flagged'
        ORDER BY changed_at ASC
        """,
        (issue_key,),
    )
    rows = cur.fetchall()

    intervals: list[tuple[datetime, Optional[datetime]]] = []
    flagged_since: Optional[datetime] = None

    for row in rows:
        changed_at = parse_ts(row["changed_at"])
        to_value = row["to_value"]

        if to_value == "Impediment" and flagged_since is None:
            flagged_since = changed_at
        elif (to_value is None or to_value == "") and flagged_since is not None:
            intervals.append((flagged_since, changed_at))
            flagged_since = None
            
    # Still flagged at end of history
    if flagged_since is not None:
        intervals.append((flagged_since, None))

    return intervals


def flagged_minutes_in_segment(
        seg_start: datetime,
        seg_end: Optional[datetime],
        flagged_intervals: list[tuple[datetime, Optional[datetime]]],
        cal: BusinessCalendar,
        now: datetime,
) -> int:
    '''
    Compute business minutes wihtin a segment that overlap with flagged intervals.
    '''
    effective_end = seg_end if seg_end is not None else now

    total = 0
    for flag_start, flag_end in flagged_intervals:
        effective_flag_end = flag_end if flag_end is not None else now

        overlap_start = max(seg_start, flag_start)
        overlap_end = min(effective_end, effective_flag_end)

        if overlap_end > overlap_start:
            total += cal.business_minutes(overlap_start, overlap_end)

    return total

def reconstruct_segments_for_issue(
        conn: sqlite3.Connection,
        issue_key: str,
        created_at: str, 
        cal: BusinessCalendar,
        now: datetime,
) -> list[dict]:
    """
    For a single issue, reconstruct all status segments from changelog events.
    Returns a list of dicts ready to inster into status_segments
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT changed_at, from_value, to_value
        FROM changelog_events
        WHERE issue_key = ? AND field = 'status'
        ORDER BY changed_at ASC
        """,
        (issue_key,),
    )
    rows = cur.fetchall()

    if not rows:
        return []

    flagged_intervals = get_flagged_intervals(conn.cursor(), issue_key)

    initial_ts = parse_ts(created_at)
    initial_stats = rows[0]["from_value"]

    transitions: list[tuple[datetime, str]] = [(initial_ts), initial_status]

    for row in rows:
        ts = parse_ts(row["changed_at"])
        to_status = row["to_value"]
        prev_ts, _ = transitions[-1]
        if ts == prev_ts:
            transitions[-1] = (ts, to_status)
        else:
            transitions.append((ts, to_status))
    
    segments = []

    for i, (seg_start, status_name) in enumerate(transitions):
        seg_end = transitions[i + 1][0] if i + 1 < len(transitions) else None
        effective_end = seg_end if seg_end is not None else now

        cal_mins = cal.calendar_minutes(seg_start, effective_end)
        bus_mins = cal.business_minutes(seg_start, effective_end)
        flag_mins = flagged_minutes_in_segment(
            seg_start, seg_end, flagged_intervals, cal
        )

        segments.append({
            "issue_key": issue_key,
            "status_name": status_name, 
            "start_ts": seg_start.isoformat(),
            "end_ts": seg_end.isoformat() if seg_end else None,
            "calendar_minutes": cal_mins,
            "business_minutes": bus_mins,
            "flagged_minutes": flag_mins,
        })

    return segments

def insert_segments(conn: sqlite3.Connection, segments: list[dict]) -> None:
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO status_segments (
        issue_key, status_name, start_ts, end_ts,
        calendar_minutes, business_minutes, flagged_minutes
        ) VALUES (
            :issue_key, :status_name, :start_ts, :end_ts,
            :calendar_minutes, :business_minutes, :flagged_minutes        
        )
        """,
        segments,
    )
    conn.commit()

def rebuild_all_segments(profile_name: str) -> dict:
    """
    Full rebuild: clears status_segments and reconstructs from scratch.
    Returns a summary dict with counts.
    """
    profile = load_profile(profile_name)
    cal = BusinessCalendar.from_profile(profile)
    conn = get_connection(profile_name)
    cur = conn.cursor()

    now = datetime.now(timezone.utc)

    cur.execute("DELETE FROM status_segments")
    conn.commit()

    cur.execute("SELECT issue_key, created_at, status_name FROM issues")
    issues = list(cur.fetchall())

    total_segments = 0
    total_issues = 0
    skipped = 0

    for issue in issues:
        issue_key = issue["issue_key"]
        created_at = issue["created_at"]
        current_status = issue["status_name"]

        if not created_at:
            skipped += 1
            continue

        segments = reconstruct_segments_for_issue(
            conn, issue_key, created_at, cal
        )

        if not segments and current_status:
            initial_ts = parse_ts(created_at)
            flagged_intervals = get_flagged_intervals(conn.cursor(), issue_key)
            flag_mins = flagged_minutes_in_segment(
                initial_ts, None, flagged_intervals, cal
            )
            segments = [{
                "issue_key": issue_key,
                "status_name": current_status,
                "start_ts": initial_ts.isoformat(),
                "end_ts": None,
                "calendar_minutes": cal.calendar_minutes(initial_ts, now),
                "business_minutes": cal.business_minutes(initial_ts, now),
                "flagged_minutes": flag_mins,
            }]

        insert_segments(conn, segments)
        total_segments += len(segments)
        total_issues += 1

    conn.close()

    return {
        "issues_processed": total_issues,
        "segments_created": total_segments,
        "issues_skipped": skipped,
    }

