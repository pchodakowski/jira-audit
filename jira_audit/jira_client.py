from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import time

import httpx

@dataclass(frozen=True)
class JiraClient:
    base_url: str
    email: str
    token: str

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=self.base_url,
            timeout=60,
            headers={"Accept": "application/json"},
            auth=(self.email, self.token), 
        )

    def search_issues(
            self,
            jql: str,
            fields: list[str],
            expand: str | None = None,
            start_at: int = 0,
            max_results: int = 100,
            next_page_token: str | None = None,
    ) -> dict:
        """
        Calls Jira Cloud: GET /rest/api/3/search
        Handles basic retry/backoff on 429/503
        """
        params: dict = {
            "maxResults": max_results,
            "fields": ",".join(fields),
            "jql": jql,
        }
        if next_page_token:
            params["nextPageToken"] = next_page_token
        else:
            params["startAt"] = start_at

        if expand:
            params["expand"] = expand

        backoff = 1.0
        with self._client() as c:
            while True:
                r = c.get("/rest/api/3/search/jql", params=params)
                if r.status_code in (429,503):
                    retry_after = r.headers.get("Retry-After")
                    sleep_s = float(retry_after) if retry_after else backoff
                    backoff = min(backoff * 2, 30)
                    time.sleep(sleep_s)
                    continue
                r.raise_for_status()
                return r.json()
