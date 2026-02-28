from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from .config import DATA_DIR, ensure_app_dirs

def db_path(profile_name: str) -> Path:
    ensure_app_dirs()
    return DATA_DIR / f"{profile_name}.sqlite"

def get_connection(profile_name: str) -> sqlite3.Connection:
    path = db_path(profile_name)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn

def initialize_db(profile_name: str) -> None:

    conn = get_connection(profile_name)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS issues (
            issue_key TEXT PRIMARY KEY,
            issue_id TEXT,
            issue_type TEXT,
            created_at TEXT,
            updated_at TEXT,
            resolutiondate TEXT,
            status_name TEXT,
            assignee_display TEXT,
            raw_json TEXT
        )
    """)

    cur.execute("""
            CREATE TABLE IF NOT EXISTS changelog_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_key TEXT,
            changed_at TEXT,
            field TEXT
            from_value TEXT,
            to_value TEXT
            )
        """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            finished_at TEXT,
            issues_count INTEGER,
            events_count INTEGER,
            error TEXT
        )
    """)
    conn.commit()
    conn.close()
    ensure_indexes(profile_name)

def ensure_indexes(profile_name: str) -> None:
    conn = get_connection(profile_name)
    cur = conn.cursor()
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_changelog_dedupe ON changelog_events(issue_key, changed_at, field, COALESCE(from_value, ''), COALESCE(to_value,''))
    """)
    conn.commit()
    conn.close()

def upsert_issue(profile_name: str, issue: dict) -> None:
    conn = get_connection(profile_name)
    cur = conn.cursor()

    fields = issue.get("fields") or {}

    assignee = fields.get("assignee") or {}
    assignee_display = assignee.get("displayName")

    sql = """
    INSERT INTO issues (
        issue_key,
        issue_id,
        issue_type,
        created_at,
        updated_at,
        resolutiondate,
        status_name,
        assignee_display,
        raw_json
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(issue_key) DO UPDATE SET
        issue_id = excluded.issue_id,
        issue_type = excluded.issue_type,
        created_at = excluded.created_at,
        updated_at = excluded.updated_at,
        resolutiondate = excluded.resolutiondate,
        status_name = excluded.status_name,
        assignee_display = excluded.assignee_display,
        raw_json = excluded.raw_json
    ;
    """

    cur.execute(
       sql,
        (
            issue.get("key"),
            issue.get("id"),
            (fields.get("issuetype") or {}).get("name"),
            fields.get("created"),
            fields.get("updated"),
            fields.get("resolutiondate"),
            (fields.get("status") or {}).get("name"),
            assignee_display,
            json.dumps(issue),
        ),
    )

    conn.commit()
    conn.close()

def insert_changelog_event(
    profile_name: str,
    issue_key: str,
    changed_at: str,
    field: str,
    from_value: str | None,
    to_value: str | None,
) -> None:
    conn = get_connection(profile_name)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO changelog_events(issue_key, changed_at, field, from_value, to_value)
            VALUES(?,?,?,?,?)
            """,
            (issue_key, changed_at, field, from_value, to_value),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()
