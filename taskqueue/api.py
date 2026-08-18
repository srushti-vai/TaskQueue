from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import ValidationError
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from .database import get_session, init_db
from .metrics import snapshot
from .models import Job, JobState
from .queue_service import (
    InvalidLease,
    InvalidTransition,
    QueueConflict,
    cancel,
    complete,
    fail,
    heartbeat,
    lease,
    manual_retry,
    submit,
)
from .schemas import (
    CompleteRequest,
    FailRequest,
    HeartbeatRequest,
    JobSubmit,
    JobView,
    LeaseRequest,
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(title="TaskQueue", version="0.1.0", lifespan=lifespan)


@app.get("/")
def root():
    return {"name": "TaskQueue", "docs": "/docs", "health": "/health", "metrics": "/metrics"}


def require_job(job_id: str, session: Session) -> Job:
    job = session.get(Job, job_id)
    if not job: raise HTTPException(404, "job not found")
    return job


@app.post("/jobs", response_model=JobView)
def create_job(data: JobSubmit, session: Session = Depends(get_session)):
    try: return submit(session, data)
    except ValueError as exc: raise HTTPException(422, str(exc)) from exc
    except ValidationError as exc: raise HTTPException(422, exc.errors()) from exc
    except QueueConflict as exc: raise HTTPException(409, str(exc)) from exc


@app.get("/jobs/{job_id}", response_model=JobView)
def get_job(job_id: str, session: Session = Depends(get_session)): return require_job(job_id, session)


@app.get("/jobs", response_model=list[JobView])
def list_jobs(state: JobState | None = None, job_type: str | None = None,
              limit: int = Query(100, ge=1, le=1000), session: Session = Depends(get_session)):
    query = select(Job).order_by(Job.created_at.desc()).limit(limit)
    if state: query = query.where(Job.state == state)
    if job_type: query = query.where(Job.job_type == job_type)
    return session.scalars(query).all()


@app.post("/workers/lease", response_model=JobView | None)
def lease_job(data: LeaseRequest, session: Session = Depends(get_session)): return lease(session, data)


@app.post("/jobs/{job_id}/heartbeat", response_model=JobView)
def beat(job_id: str, data: HeartbeatRequest, session: Session = Depends(get_session)):
    try:
        return heartbeat(session, require_job(job_id, session), data.worker_id,
                         data.lease_token, data.lease_seconds)
    except InvalidLease as exc: raise HTTPException(409, str(exc)) from exc


@app.post("/jobs/{job_id}/complete", response_model=JobView)
def finish(job_id: str, data: CompleteRequest, session: Session = Depends(get_session)):
    try: return complete(session, require_job(job_id, session), data.worker_id, data.lease_token, data.result)
    except InvalidLease as exc: raise HTTPException(409, str(exc)) from exc


@app.post("/jobs/{job_id}/fail", response_model=JobView)
def report_failure(job_id: str, data: FailRequest, session: Session = Depends(get_session)):
    try: return fail(session, require_job(job_id, session), data.worker_id, data.lease_token, data.retryable, data.error)
    except InvalidLease as exc: raise HTTPException(409, str(exc)) from exc


@app.post("/jobs/{job_id}/cancel", response_model=JobView)
def cancel_job(job_id: str, session: Session = Depends(get_session)):
    try: return cancel(session, require_job(job_id, session))
    except InvalidTransition as exc: raise HTTPException(409, str(exc)) from exc


@app.post("/jobs/{job_id}/retry", response_model=JobView)
def retry_job(job_id: str, session: Session = Depends(get_session)):
    try: return manual_retry(session, require_job(job_id, session))
    except InvalidTransition as exc: raise HTTPException(409, str(exc)) from exc


@app.get("/health")
def health(session: Session = Depends(get_session)):
    session.execute(text("SELECT 1")); return {"status": "ready", "database": "ready"}


@app.get("/metrics")
def metrics(session: Session = Depends(get_session)): return snapshot(session)
