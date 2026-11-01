"""Minimal client for the Airbyte public API (v1), used by the Airflow DAG and scripts/airbyte_bootstrap.py.

Auth uses an Airbyte application's client id/secret (`abctl local credentials` prints them for a local
install). Only the calls this project needs are wrapped: workspaces, sources, destinations,
connections, and triggering + polling a sync job.
"""
from __future__ import annotations

import os
import time

import requests

TERMINAL_OK = {"succeeded"}
TERMINAL_FAIL = {"failed", "cancelled", "incomplete"}


class AirbyteError(RuntimeError):
    pass


class AirbyteClient:
    def __init__(self, api_url: str | None = None, client_id: str | None = None,
                 client_secret: str | None = None, timeout: int = 30):
        self.api_url = (api_url or os.environ["AIRBYTE_API_URL"]).rstrip("/")
        self.client_id = client_id or os.environ.get("AIRBYTE_CLIENT_ID", "")
        self.client_secret = client_secret or os.environ.get("AIRBYTE_CLIENT_SECRET", "")
        self.timeout = timeout
        self._token: str | None = None
        self._token_expires = 0.0

    def _auth_header(self) -> dict:
        if not self.client_id:
            return {}
        if not self._token or time.time() > self._token_expires:
            resp = requests.post(
                f"{self.api_url}/applications/token",
                json={"client_id": self.client_id, "client_secret": self.client_secret,
                      "grant-type": "client_credentials"},
                timeout=self.timeout,
            )
            self._raise_for_status(resp)
            body = resp.json()
            self._token = body["access_token"]
            # Tokens last a few minutes; refresh a minute early.
            self._token_expires = time.time() + int(body.get("expires_in", 180)) - 60
        return {"Authorization": f"Bearer {self._token}"}

    @staticmethod
    def _raise_for_status(resp: requests.Response) -> None:
        if resp.status_code >= 400:
            raise AirbyteError(f"{resp.request.method} {resp.url} -> {resp.status_code}: {resp.text[:500]}")

    def request(self, method: str, path: str, **kwargs) -> dict:
        resp = requests.request(method, f"{self.api_url}{path}", headers=self._auth_header(),
                                timeout=self.timeout, **kwargs)
        self._raise_for_status(resp)
        return resp.json() if resp.content else {}

    def _list(self, path: str, **params) -> list[dict]:
        items, offset = [], 0
        while True:
            page = self.request("GET", path, params={**params, "limit": 100, "offset": offset})
            items.extend(page.get("data", []))
            if not page.get("next"):
                return items
            offset += 100

    def default_workspace_id(self) -> str:
        workspaces = self._list("/workspaces")
        if not workspaces:
            raise AirbyteError("no Airbyte workspace found")
        return workspaces[0]["workspaceId"]

    def find_by_name(self, kind: str, name: str, workspace_id: str) -> dict | None:
        """kind is 'sources', 'destinations' or 'connections'."""
        for item in self._list(f"/{kind}", workspaceIds=workspace_id):
            if item.get("name") == name:
                return item
        return None

    def connection_id(self, name: str) -> str:
        connection = self.find_by_name("connections", name, self.default_workspace_id())
        if not connection:
            raise AirbyteError(f"Airbyte connection {name!r} not found; run scripts/airbyte_bootstrap.py")
        return connection["connectionId"]

    def trigger_sync(self, connection_id: str) -> int:
        job = self.request("POST", "/jobs", json={"connectionId": connection_id, "jobType": "sync"})
        return job["jobId"]

    def wait_for_job(self, job_id: int, timeout_s: int = 3600, poll_s: int = 10, log=print) -> dict:
        deadline = time.time() + timeout_s
        while True:
            job = self.request("GET", f"/jobs/{job_id}")
            status = job.get("status")
            log(f"airbyte job {job_id}: {status} rows={job.get('rowsSynced')} bytes={job.get('bytesSynced')}")
            if status in TERMINAL_OK:
                return job
            if status in TERMINAL_FAIL:
                raise AirbyteError(f"airbyte job {job_id} ended with status {status}")
            if time.time() > deadline:
                raise AirbyteError(f"airbyte job {job_id} still {status} after {timeout_s}s")
            time.sleep(poll_s)
