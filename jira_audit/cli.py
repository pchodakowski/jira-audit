from __future__ import annotations

import getpass
from typing import Optional

import httpx
import typer
from rich import print
from datetime import datetime, timedelta, timezone

from .jira_client import JiraClient
from .config import ClientProfile, save_profile, load_profile
from .auth import save_token, load_token
from .db import initialize_db, upsert_issue, insert_changelog_event, db_path

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
def sync(
    profile: str,
    limit: int =typer.Option(0, help="Optional max issues to fetch (0 = no limit)."),
    days: int = typer.Option(365, help="How may days back to sync by default."),
):
    """
    Pull Jira issues (with changelog) into local SQLite cache.
    """
    prof = load_profile(profile)
    token = load_token(profile, prof.jira_base_url)
    if not token:
        raise typer.BadParameter(f"No token found in keychain for profile '{profile}'. Run configure again.")
    
    initialize_db(profile)
    print(f"[cyan]Using DB: [/cyan] {db_path(profile)}")

    jira = JiraClient(base_url=prof.jira_base_url, email=prof.email, token=token)

    since = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()

    jql = (
        f'project = "{prof.project_key}" '
        f'AND issuetype != "Sub-task" '
        f'AND (updated >= "{since}" OR created >= "{since}") '
        f'ORDER BY updated ASC'
    )

    fields = [
        "created",
        "updated",
        "resolutiondate",
        "issuetype",
        "status",
        "assignee",
        "priority",
        "labels",
        "components",
    ]

    start_at = 0
    page_size = 100
    total_seen = 0
    total_events = 0

    while True:
        data = jira.search_issues(
            jql=jql,
            fields=fields,
            expand="changelog",
            start_at=start_at,
            max_results=page_size,
        )

        issues = data.get("issues", []) or []
        if not issues:
            break

        for issue in issues:
            issue_key = issue.get("key")
            if not issue_key:
                continue

            upsert_issue(profile, issue)
            total_seen += 1

            changelog = issue.get("changelog") or {}
            histories = changelog.get("histories", []) or []
            for h in histories:
                changed_at = h.get("created")
                items = h.get("items", []) or []
                for item in items:
                    field = item.get("field")
                    if field not in ("status", "Flagged", "assignee"):
                        continue

                    from_str = item.get("fromString")
                    to_str = item.get("toString")

                    insert_changelog_event(
                        profile_name=profile,
                        issue_key=issue_key,
                        changed_at=changed_at,
                        field=field,
                        from_value=from_str,
                        to_value=to_str,
                    )
                    total_events +=1
            if limit and total_seen >= limit:
                print(f"[yellow]Limit reached:[/yellow] {limit} issues")
                print(f"[green]Synced issues:[/green] {total_seen} [green]events:[/green] {total_events}")
                return
            
        start_at += len(issues)
        total = data.get("total", 0)
        print(f"Fetched {start_at}/{total} issues...")

        if start_at >= total:
            break

    print(f"[green]Done.[/green] Synced issues: {total_seen} events: {total_events}")

if __name__ == "__main__":
    app()
