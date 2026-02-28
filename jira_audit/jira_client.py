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
            fields: List[str],
            expand: Optional[str] = None,
            start_at: int = 0,
            max_results: int = 100,
    ) -> Dict[str, Any]:
        """
        Calls Jira Cloud: GET /rest/api/3/search
        Handles basic retry/backoff on 429/503
        """
        params: Dict[str, Any] = {
            "jql": jql,
            "startAt": start_at,
            "maxResults": max_results,
            "fields": ",".join(fields),
        }
        if expand:
            params["expand"] = expand

        backoff = 1.0
        with self._client() as c:
            while True:
                r = c.get("/rest/api/3/search/jql", params=params)
                if r.status_code in (429,503):
                    retry_after = r.headers.get("Retry-After")
                    if retry_after:
                        sleep_s = float(retry_after)
                    else:
                        sleep_s = backoff
                        backoff = min(backoff * 2, 30)
                    time.sleep(sleep_s)
                    continue
                r.raise_for_status()
                return r.json()
