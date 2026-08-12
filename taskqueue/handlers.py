import asyncio
import csv
import hashlib
import hmac
import ipaddress
import json
import socket
from urllib.parse import urlsplit

import httpx

from .config import settings
from .schemas import DeliverWebhookPayload, GenerateReportPayload, SimulateFailurePayload


class RetryableJobError(Exception): pass
class PermanentJobError(Exception): pass


async def generate_report(payload: dict, _attempt: int) -> dict:
    data = GenerateReportPayload.model_validate(payload)
    output = settings.output_dir.resolve(); output.mkdir(parents=True, exist_ok=True)
    filename = f"customer-{data.customer_id}-{data.row_count}.csv"
    path = (output / filename).resolve()
    if output not in path.parents:
        raise PermanentJobError("unsafe report path")
    total = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["row_id", "customer_id", "amount"])
        writer.writeheader()
        for row_id in range(1, data.row_count + 1):
            amount = (data.customer_id * 17 + row_id * 13) % 1000
            total += amount
            writer.writerow({"row_id": row_id, "customer_id": data.customer_id, "amount": amount})
    return {"path": str(path), "row_count": data.row_count, "total_amount": total}


def webhook_signature(body: dict, delivery_id: str, secret: str = settings.webhook_secret) -> str:
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), delivery_id.encode() + b"." + encoded, hashlib.sha256).hexdigest()


def _validate_target(url: str) -> None:
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or parts.username or parts.password or not parts.hostname:
        raise PermanentJobError("invalid webhook URL")
    try:
        addresses = {ipaddress.ip_address(item[4][0]) for item in socket.getaddrinfo(parts.hostname, parts.port or 80)}
    except socket.gaierror as exc:
        raise RetryableJobError("webhook host could not be resolved") from exc
    for address in addresses:
        unsafe = address.is_private or address.is_loopback or address.is_link_local or address.is_reserved
        if unsafe and not (settings.allow_loopback_webhooks and address.is_loopback):
            raise PermanentJobError("webhook target is not allowed")


async def deliver_webhook(payload: dict, _attempt: int) -> dict:
    data = DeliverWebhookPayload.model_validate(payload); url = str(data.target_url)
    _validate_target(url)
    delivery_id = hashlib.sha256(data.event_id.encode()).hexdigest()[:32]
    headers = {"X-TaskQueue-Delivery": delivery_id,
               "X-TaskQueue-Signature": webhook_signature(data.body, delivery_id)}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(url, json=data.body, headers=headers)
    except (httpx.TimeoutException, httpx.NetworkError) as exc:
        raise RetryableJobError("webhook delivery failed") from exc
    if response.status_code == 429 or response.status_code >= 500:
        raise RetryableJobError(f"webhook returned HTTP {response.status_code}")
    if 400 <= response.status_code < 500:
        raise PermanentJobError(f"webhook returned HTTP {response.status_code}")
    return {"delivery_id": delivery_id, "status_code": response.status_code,
            "response_preview": response.text[:1024]}


async def simulate_failure(payload: dict, attempt: int) -> dict:
    data = SimulateFailurePayload.model_validate(payload)
    await asyncio.sleep(data.duration_seconds)
    if attempt <= data.fail_attempts:
        raise RetryableJobError(f"planned failure {attempt}/{data.fail_attempts}")
    return {"attempt": attempt, "message": "simulation succeeded"}


HANDLERS = {"generate_report": generate_report, "deliver_webhook": deliver_webhook,
            "simulate_failure": simulate_failure}

