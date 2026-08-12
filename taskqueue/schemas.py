from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl

from .models import JobState


class GenerateReportPayload(BaseModel):
    customer_id: int
    row_count: int = Field(ge=1, le=100_000)
    format: Literal["csv"] = "csv"


class DeliverWebhookPayload(BaseModel):
    event_id: str
    target_url: HttpUrl
    body: dict[str, Any]


class SimulateFailurePayload(BaseModel):
    fail_attempts: int = Field(ge=0, le=100)
    duration_seconds: float = Field(default=0, ge=0, le=300)


PAYLOAD_MODELS = {
    "generate_report": GenerateReportPayload,
    "deliver_webhook": DeliverWebhookPayload,
    "simulate_failure": SimulateFailurePayload,
}


class JobSubmit(BaseModel):
    job_type: str
    payload: dict[str, Any]
    max_attempts: int = Field(default=3, ge=1, le=100)
    idempotency_key: str | None = Field(default=None, max_length=128)


class JobView(BaseModel):
    id: str
    job_type: str
    payload: dict[str, Any]
    state: JobState
    attempt_count: int
    max_attempts: int
    available_at: datetime
    lease_owner: str | None
    lease_token: str | None
    lease_expires_at: datetime | None
    idempotency_key: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    result: dict[str, Any] | None
    last_error: str | None
    model_config = {"from_attributes": True}


class LeaseRequest(BaseModel):
    worker_id: str
    supported_job_types: list[str]
    lease_seconds: float = Field(default=10, ge=0.05, le=3600)


class LeaseProof(BaseModel):
    worker_id: str
    lease_token: str


class CompleteRequest(LeaseProof):
    result: dict[str, Any] = {}


class FailRequest(LeaseProof):
    retryable: bool
    error: str = Field(max_length=1000)

