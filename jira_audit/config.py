from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
import yaml

APP_DIR = Path.home() / ".jira-audit"
CLIENTS_DIR = APP_DIR / "clients"
DATA_DIR = APP_DIR / "data"
REPORTS_DIR = APP_DIR / "reports"

def ensure_app_dirs() -> None:
        CLIENTS_DIR.mkdir(parents=True, exist_ok=True)
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)

@dataclass
class ClientProfile:
            name: str
            jira_base_url: str
            email: str
            project_key: str
            timezone: str

            business_hours_start: str = "09:00"
            business_hours_end: str = "17:00"
            holidays: list[str] | None = None

            #Per-client status rollups
            status_rollups: dict[str, list[str]] | None = None
            
            #Blocked / flagged settings
            use_flagged: bool = True
            flagged_value : str = "Impediment"
            blocked_status_names: list[str] | None = None

def profile_path(profile_name: str) -> Path:
        ensure_app_dirs()
        return CLIENTS_DIR / f"{profile_name}.yaml"

def save_profile(profile: ClientProfile) -> None:
        ensure_app_dirs()
        p = profile_path(profile.name)
        with p.open("w", encoding="utf-8") as f:
                yaml.safe_dump(asdict(profile), f, sort_keys=False)

def load_profile(profile_name: str) -> ClientProfile:
        p = profile_path(profile_name)
        if not p.exists():
            raise FileNotFoundError(
                f"Profile '{profile_name}' not found at {p}. Run: python -m jira-audit.cli configure {profile_name}"
            )
        with p.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return ClientProfile(**data)
