import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .config import settings
from .models import Job, JobState
from .retry import retry_delay
from .schemas import PAYLOAD_MODELS, JobSubmit, LeaseRequest


class QueueConflict(Exception): pass
class InvalidLease(Exception): pass
class InvalidTransition(Exception): pass


def now() -> datetime:
    return datetime.now(UTC)


def _hash(data: JobSubmit) -> str:
    value = {"job_type": data.job_type, "payload": data.payload}
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def submit(session: Session, data: JobSubmit) -> Job:
    model = PAYLOAD_MODELS.get(data.job_type)
    if not model:
        raise ValueError("unknown job type")
    payload = model.model_validate(data.payload).model_dump(mode="json")
    normalized = data.model_copy(update={"payload": payload})
    request_hash = _hash(normalized)
    if data.idempotency_key:
        existing = session.scalar(select(Job).where(Job.idempotency_key == data.idempotency_key))
        if existing:
            if existing.request_hash != request_hash:
                raise QueueConflict("idempotency key was already used for different content")
            return existing
    job = Job(job_type=data.job_type, payload=payload, max_attempts=data.max_attempts,
              idempotency_key=data.idempotency_key, request_hash=request_hash)
    session.add(job)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        existing = session.scalar(select(Job).where(Job.idempotency_key == data.idempotency_key))
        if existing and existing.request_hash == request_hash:
            return existing
        raise QueueConflict("conflicting idempotency request")
    return job


def recover(session: Session) -> int:
    stamp = now()
    expired = session.execute(update(Job).where(Job.state == JobState.LEASED,
        Job.lease_expires_at < stamp).values(state=JobState.QUEUED, lease_owner=None,
        lease_token=None, lease_expires_at=None, available_at=stamp, updated_at=stamp)).rowcount
    session.execute(update(Job).where(Job.state == JobState.RETRY_WAIT, Job.available_at <= stamp)
        .values(state=JobState.QUEUED, updated_at=stamp))
    session.commit()
    return expired


def lease(session: Session, request: LeaseRequest) -> Job | None:
    recover(session)
    stamp = now()
    ids = session.scalars(select(Job.id).where(Job.state == JobState.QUEUED,
        Job.available_at <= stamp, Job.job_type.in_(request.supported_job_types))
        .order_by(Job.created_at).limit(10)).all()
    for job_id in ids:
        token = str(uuid.uuid4())
        result = session.execute(update(Job).where(Job.id == job_id, Job.state == JobState.QUEUED)
            .values(state=JobState.LEASED, lease_owner=request.worker_id, lease_token=token,
                    lease_expires_at=stamp + timedelta(seconds=request.lease_seconds),
                    attempt_count=Job.attempt_count + 1, started_at=stamp, updated_at=stamp))
        session.commit()
        if result.rowcount == 1:
            return session.get(Job, job_id, populate_existing=True)
    return None


def prove(job: Job | None, worker: str, token: str) -> Job:
    if not job or job.state != JobState.LEASED or job.lease_owner != worker or job.lease_token != token:
        raise InvalidLease("stale or invalid lease")
    return job


def heartbeat(session: Session, job: Job, worker: str, token: str, seconds: float = 10) -> Job:
    prove(job, worker, token)
    job.lease_expires_at = now() + timedelta(seconds=seconds)
    job.updated_at = now(); session.commit(); return job


def complete(session: Session, job: Job, worker: str, token: str, result: dict) -> Job:
    prove(job, worker, token); stamp = now()
    job.state = JobState.SUCCEEDED; job.result = result; job.completed_at = stamp
    job.updated_at = stamp; job.lease_owner = job.lease_token = job.lease_expires_at = None
    session.commit(); return job


def fail(session: Session, job: Job, worker: str, token: str, retryable: bool, error: str) -> Job:
    prove(job, worker, token); stamp = now(); job.last_error = error[:1000]
    if retryable and job.attempt_count < job.max_attempts:
        job.state = JobState.RETRY_WAIT
        job.available_at = stamp + timedelta(seconds=retry_delay(job.attempt_count,
            settings.retry_base_seconds, settings.retry_max_seconds))
    else:
        job.state = JobState.DEAD_LETTER if retryable else JobState.FAILED
        job.completed_at = stamp
    job.updated_at = stamp; job.lease_owner = job.lease_token = job.lease_expires_at = None
    session.commit(); return job


def cancel(session: Session, job: Job) -> Job:
    if job.state not in {JobState.QUEUED, JobState.RETRY_WAIT}:
        raise InvalidTransition("only queued or waiting jobs may be cancelled")
    job.state = JobState.CANCELLED; job.updated_at = now(); session.commit(); return job


def manual_retry(session: Session, job: Job) -> Job:
    if job.state not in {JobState.DEAD_LETTER, JobState.FAILED}:
        raise InvalidTransition("only failed or dead-letter jobs may be retried")
    job.state = JobState.QUEUED; job.available_at = now(); job.completed_at = None
    job.lease_owner = job.lease_token = job.lease_expires_at = None
    job.updated_at = now(); session.commit(); return job
