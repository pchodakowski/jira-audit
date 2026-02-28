from __future__ import annotations

import getpass
from typing import Optional

import httpx
import typer
from rich import print

from .config import ClientProfile, save_profile, load_profile
from .auth import save_token, load_token
from .db import initialize_db

app = typer.Typer(add_completion=False)

def normalize_base_url(url: str) -> str:
    url = url.strip()
    if url.endswith("/"):
        url = url[:-1]
    return url

@app.command()
def configure(profile: str):
    """
    Create/update a client profile and store its Jira API token in macOS Keychain.
    """
    jira_base_url = normalize_base_url(typer.prompt("Jira base URL (e.g. https://client.atlassian.net)"))
    email = typer.prompt("Jira e-mail (the accoune used with the API token)")
    project_key = typer.prompt("Project key (e.g. ABC)")
    timezone = typer.prompt("Timezone (IANA, e.g. America/New_York)", default="America/New_York")

    token = getpass.getpass("Jira API token (input hidden): ").strip()
    if not token:
        raise typer.BadParameter("Token cannot be empty.")
    
    profile_obj = ClientProfile(
        name=profile,
        jira_base_url=jira_base_url,
        email=email,
        project_key=project_key,
        timezone=timezone,
        holidays=[],
        status_rollups={
            "Backlog": [],
            "InProgress": [],
            "Review": [],
            "Blocked": [],
            "Done": [],
        },
        blocked_status_names=[],
    )

    save_profile(profile_obj)
    save_token(profile, jira_base_url, token)

    print(f"[green]Saved profile[/green] {profile} and stored token in Keychain.")

@app.command()
def whoami(profile: str):
    """
    Validate credentials by calling Jira Cloud 'myself' endpoint.
    """
    prof = load_profile(profile)
    token = load_token(profile, prof.jira_base_url)
    if not token:
        raise typer.BadParameter(
            f"No token found in keychain for profile '{profile}'. Run configure again."
        )
    
    url = f"{prof.jira_base_url}/rest/api/3/myself"

    # Jira Cloud API token auth: Basic base64(email:token)
    auth = (prof.email, token)

    with httpx.Client(timeout=30) as client:
        r = client.get(url, auth=auth, headers={"Accept": "application/json"})
        if r.status_code >= 400:
            print("[red]Auth failed.[/red]")
            print(f"Status: {r.status_code}")
            print(r.text)
            raise typer.Exit(code=1)
        
        data = r.json()
        display = data.get("displayName")
        acct_id = data.get("accountID")
        print("[green]Auth OK[/green]")
        print(f"User: {display}")
        print(f"AccountId: {acct_id}")

@app.command()
def sync(profile: str):
    """
    Initialize local database for profile.
    (Next step pull issues from Jira.)
    """
    initialize_db(profile)
    print(f"[green]Database initialized for profile[/green] {profile}")

if __name__ == "__main__":
    app()
