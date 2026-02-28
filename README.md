# jira-audit

Kanban flow audit tool for Jira Cloud.

## Quickstart (macOS/Linux)
```bash
git clone <REPO_URL>
cd jira-audit
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
jira-audit configure <profile>
jira-audit whoami <profile>