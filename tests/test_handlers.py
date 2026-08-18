import hashlib
import hmac
import json
import socket

import httpx
import pytest

from taskqueue import handlers
from taskqueue.config import Settings
from taskqueue.handlers import PermanentJobError, RetryableJobError


@pytest.mark.asyncio
async def test_generate_report_is_deterministic_and_controlled(tmp_path, monkeypatch):
    monkeypatch.setattr(handlers, "settings", Settings(output_dir=tmp_path))
    payload = {"customer_id": 42, "row_count": 3, "format": "csv"}
    first = await handlers.generate_report(payload, 1)
    contents = (tmp_path / "customer-42-3.csv").read_text(encoding="utf-8")
    second = await handlers.generate_report(payload, 2)
    assert first == second
    assert first["row_count"] == 3
    assert "customer_id" in contents
    assert (tmp_path / "customer-42-3.csv").resolve().is_relative_to(tmp_path.resolve())


def mock_dns(monkeypatch, address="127.0.0.1"):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *_args, **_kwargs: [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 9000))])


def mock_http(monkeypatch, handler):
    original = httpx.AsyncClient
    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(handlers.httpx, "AsyncClient",
                        lambda **kwargs: original(transport=transport, **kwargs))


@pytest.mark.asyncio
async def test_webhook_503_is_retryable(monkeypatch):
    mock_dns(monkeypatch)
    mock_http(monkeypatch, lambda _request: httpx.Response(503, text="unavailable"))
    with pytest.raises(RetryableJobError, match="503"):
        await handlers.deliver_webhook({"event_id": "evt-1",
            "target_url": "http://localhost:9000/webhook", "body": {"ok": True}}, 1)


@pytest.mark.asyncio
async def test_webhook_400_is_permanent(monkeypatch):
    mock_dns(monkeypatch)
    mock_http(monkeypatch, lambda _request: httpx.Response(400, text="bad request"))
    with pytest.raises(PermanentJobError, match="400"):
        await handlers.deliver_webhook({"event_id": "evt-1",
            "target_url": "http://localhost:9000/webhook", "body": {"ok": True}}, 1)


@pytest.mark.asyncio
async def test_webhook_network_error_is_retryable(monkeypatch):
    mock_dns(monkeypatch)

    def timeout(request):
        raise httpx.ConnectError("offline", request=request)

    mock_http(monkeypatch, timeout)
    with pytest.raises(RetryableJobError, match="delivery failed"):
        await handlers.deliver_webhook({"event_id": "evt-1",
            "target_url": "http://localhost:9000/webhook", "body": {}}, 1)


@pytest.mark.asyncio
async def test_webhook_blocks_private_networks(monkeypatch):
    mock_dns(monkeypatch, "10.0.0.8")
    with pytest.raises(PermanentJobError, match="not allowed"):
        await handlers.deliver_webhook({"event_id": "evt-1",
            "target_url": "http://internal.example/webhook", "body": {}}, 1)


@pytest.mark.asyncio
async def test_webhook_signature_is_sent_and_verifiable(monkeypatch):
    mock_dns(monkeypatch)
    captured = {}

    def receive(request):
        captured.update(request.headers)
        return httpx.Response(200, text="x" * 2048)

    mock_http(monkeypatch, receive)
    result = await handlers.deliver_webhook({"event_id": "evt-123",
        "target_url": "http://localhost:9000/webhook", "body": {"status": "done"}}, 1)
    delivery_id = hashlib.sha256(b"evt-123").hexdigest()[:32]
    encoded = json.dumps({"status": "done"}, sort_keys=True, separators=(",", ":")).encode()
    expected = hmac.new(handlers.settings.webhook_secret.encode(),
                        delivery_id.encode() + b"." + encoded, hashlib.sha256).hexdigest()
    assert captured["x-taskqueue-delivery"] == delivery_id
    assert hmac.compare_digest(captured["x-taskqueue-signature"], expected)
    assert len(result["response_preview"]) == 1024
