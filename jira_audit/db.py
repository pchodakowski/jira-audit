from __future__ import annotations

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