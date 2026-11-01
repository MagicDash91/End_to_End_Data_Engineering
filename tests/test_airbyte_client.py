"""AirbyteClient against a fake requests layer."""
import pytest

from pipeline import airbyte_client
from pipeline.airbyte_client import AirbyteClient, AirbyteError


class FakeResponse:
    def __init__(self, status_code=200, body=None, method="GET", url="http://airbyte"):
        self.status_code = status_code
        self._body = body or {}
        self.content = b"x"
        self.text = str(body)
        self.url = url
        self.request = type("Req", (), {"method": method})()

    def json(self):
        return self._body


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(airbyte_client.time, "sleep", lambda _: None)
    return AirbyteClient(api_url="http://airbyte/api/public/v1", client_id="id", client_secret="secret")


def test_token_is_fetched_once_and_sent(monkeypatch, client):
    calls = []
    monkeypatch.setattr(airbyte_client.requests, "post",
                        lambda url, **kw: calls.append(url) or FakeResponse(body={"access_token": "tok"}))
    seen_headers = []

    def fake_request(method, url, headers=None, **kw):
        seen_headers.append(headers)
        return FakeResponse(body={"data": [{"workspaceId": "ws-1"}]})

    monkeypatch.setattr(airbyte_client.requests, "request", fake_request)
    assert client.default_workspace_id() == "ws-1"
    assert client.default_workspace_id() == "ws-1"
    assert calls == ["http://airbyte/api/public/v1/applications/token"]
    assert all(h == {"Authorization": "Bearer tok"} for h in seen_headers)


def test_wait_for_job_until_success(monkeypatch, client):
    statuses = iter(["pending", "running", "succeeded"])
    monkeypatch.setattr(client, "request", lambda m, p, **kw: {"status": next(statuses), "rowsSynced": 10})
    assert client.wait_for_job(5, log=lambda _: None)["status"] == "succeeded"


def test_wait_for_job_raises_on_failure(monkeypatch, client):
    monkeypatch.setattr(client, "request", lambda m, p, **kw: {"status": "failed"})
    with pytest.raises(AirbyteError, match="failed"):
        client.wait_for_job(5, log=lambda _: None)


def test_missing_connection_is_a_clear_error(monkeypatch, client):
    monkeypatch.setattr(client, "default_workspace_id", lambda: "ws-1")
    monkeypatch.setattr(client, "_list", lambda path, **params: [{"name": "other", "connectionId": "c"}])
    with pytest.raises(AirbyteError, match="airbyte_bootstrap"):
        client.connection_id("shop-to-bronze")
