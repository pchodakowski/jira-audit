from __future__ import annotations

import keyring

SERVICE = "jira-audit"

def keychain_account(profile_name: str, jira_base_url: str) -> str:
    # Token is per Jira site; tie it to profile and base_url for safety
    return f"{profile_name}::{jira_base_url}".lower()

def save_token(profile_name: str, jira_base_url: str, token: str) -> None:
    acct = keychain_account(profile_name, jira_base_url)
    keyring.set_password(SERVICE, acct, token)

def load_token(profile_name: str, jira_base_url: str) -> str | None:
    acct = keychain_account(profile_name, jira_base_url)
    return keyring.get_password(SERVICE, acct)