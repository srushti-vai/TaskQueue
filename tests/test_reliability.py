import concurrent.futures
import time

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from taskqueue.database import init_db, make_engine
from taskqueue.models import Job, JobState
from taskqueue.queue_service import InvalidLease, complete, lease, submit
from taskqueue.schemas import JobSubmit, LeaseRequest


def failure_payload(fail_attempts=0):
    return {"fail_attempts": fail_attempts, "duration_seconds": 0}


def test_concurrent_idempotent_submissions_create_one_job(tmp_path):
    engine = make_engine(f"sqlite:///{(tmp_path / 'idempotency.db').as_posix()}")
    init_db(engine)
    factory = sessionmaker(engine, expire_on_commit=False)
    request = JobSubmit(job_type="simulate_failure", payload=failure_payload(),
                        idempotency_key="network-retry")

    def create_one(_):
        with factory() as db:
            return submit(db, request).id

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        ids = list(pool.map(create_one, range(8)))
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(Job)) == 1
    assert len(set(ids)) == 1
    engine.dispose()


def test_only_one_concurrent_completion_wins(tmp_path):
    engine = make_engine(f"sqlite:///{(tmp_path / 'completion.db').as_posix()}")
    init_db(engine)
    factory = sessionmaker(engine, expire_on_commit=False)
    with factory() as db:
        job = submit(db, JobSubmit(job_type="simulate_failure", payload=failure_payload()))
        claimed = lease(db, LeaseRequest(worker_id="worker", supported_job_types=[job.job_type]))
        job_id, token = claimed.id, claimed.lease_token

    def finish(value):
        with factory() as db:
            try:
                complete(db, db.get(Job, job_id), "worker", token, {"winner": value})
                return True
            except InvalidLease:
                return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(finish, [1, 2]))
    assert outcomes.count(True) == 1
    with factory() as db:
        assert db.get(Job, job_id).state == JobState.SUCCEEDED
    engine.dispose()


def test_jobs_survive_engine_restart(tmp_path):
    url = f"sqlite:///{(tmp_path / 'durable.db').as_posix()}"
    first_engine = make_engine(url)
    init_db(first_engine)
    first_factory = sessionmaker(first_engine, expire_on_commit=False)
    with first_factory() as db:
        job_id = submit(db, JobSubmit(job_type="simulate_failure",
                                      payload=failure_payload())).id
    first_engine.dispose()

    restarted_engine = make_engine(url)
    init_db(restarted_engine)
    restarted_factory = sessionmaker(restarted_engine, expire_on_commit=False)
    with restarted_factory() as db:
        restored = db.get(Job, job_id)
        assert restored is not None
        assert restored.state == JobState.QUEUED
    restarted_engine.dispose()


def test_expired_lease_and_retry_metrics_are_event_counts(client):
    created = client.post("/jobs", json={"job_type": "simulate_failure",
        "payload": failure_payload(), "max_attempts": 2}).json()
    first = client.post("/workers/lease", json={"worker_id": "A",
        "supported_job_types": ["simulate_failure"], "lease_seconds": 0.05}).json()
    time.sleep(0.07)
    second = client.post("/workers/lease", json={"worker_id": "B",
        "supported_job_types": ["simulate_failure"], "lease_seconds": 1}).json()
    failed = client.post(f"/jobs/{created['id']}/fail", json={"worker_id": "B",
        "lease_token": second["lease_token"], "retryable": True, "error": "temporary"})
    assert failed.status_code == 200
    metrics = client.get("/metrics").json()
    assert metrics["retry_count"] == 1
    assert metrics["expired_lease_recoveries"] == 1
    assert first["lease_token"] != second["lease_token"]


def test_expired_lease_cannot_heartbeat_or_complete(client):
    job = client.post("/jobs", json={"job_type": "simulate_failure",
        "payload": failure_payload()}).json()
    claimed = client.post("/workers/lease", json={"worker_id": "slow",
        "supported_job_types": ["simulate_failure"], "lease_seconds": 0.05}).json()
    time.sleep(0.07)
    proof = {"worker_id": "slow", "lease_token": claimed["lease_token"]}
    assert client.post(f"/jobs/{job['id']}/heartbeat", json=proof).status_code == 409
    assert client.post(f"/jobs/{job['id']}/complete",
                       json={**proof, "result": {}}).status_code == 409


def test_heartbeat_extends_the_original_lease(client):
    job = client.post("/jobs", json={"job_type": "simulate_failure",
        "payload": failure_payload()}).json()
    claimed = client.post("/workers/lease", json={"worker_id": "worker",
        "supported_job_types": ["simulate_failure"], "lease_seconds": 0.2}).json()
    extended = client.post(f"/jobs/{job['id']}/heartbeat", json={"worker_id": "worker",
        "lease_token": claimed["lease_token"], "lease_seconds": 2}).json()
    assert extended["lease_expires_at"] > claimed["lease_expires_at"]


def test_manual_retry_resets_attempt_policy(client):
    job = client.post("/jobs", json={"job_type": "simulate_failure",
        "payload": failure_payload(), "max_attempts": 1}).json()
    claimed = client.post("/workers/lease", json={"worker_id": "worker",
        "supported_job_types": ["simulate_failure"]}).json()
    dead = client.post(f"/jobs/{job['id']}/fail", json={"worker_id": "worker",
        "lease_token": claimed["lease_token"], "retryable": True, "error": "exhausted"}).json()
    assert dead["state"] == "dead_letter"
    retried = client.post(f"/jobs/{job['id']}/retry").json()
    assert retried["state"] == "queued"
    assert retried["attempt_count"] == 0
    assert retried["last_error"] is None


@pytest.mark.parametrize("state", ["succeeded", "cancelled"])
def test_terminal_jobs_are_never_leased(client, state):
    job = client.post("/jobs", json={"job_type": "simulate_failure",
        "payload": failure_payload()}).json()
    if state == "cancelled":
        client.post(f"/jobs/{job['id']}/cancel")
    else:
        claimed = client.post("/workers/lease", json={"worker_id": "worker",
            "supported_job_types": ["simulate_failure"]}).json()
        client.post(f"/jobs/{job['id']}/complete", json={"worker_id": "worker",
            "lease_token": claimed["lease_token"], "result": {}})
    assert client.post("/workers/lease", json={"worker_id": "other",
        "supported_job_types": ["simulate_failure"]}).json() is None


def test_invalid_state_transitions_return_conflict(client):
    job = client.post("/jobs", json={"job_type": "simulate_failure",
        "payload": failure_payload()}).json()
    assert client.post(f"/jobs/{job['id']}/retry").status_code == 409
    claimed = client.post("/workers/lease", json={"worker_id": "worker",
        "supported_job_types": ["simulate_failure"]}).json()
    assert client.post(f"/jobs/{job['id']}/cancel").status_code == 409
    client.post(f"/jobs/{job['id']}/complete", json={"worker_id": "worker",
        "lease_token": claimed["lease_token"], "result": {}})
    assert client.post(f"/jobs/{job['id']}/cancel").status_code == 409


def test_root_and_job_filters(client):
    assert client.get("/").json()["docs"] == "/docs"
    client.post("/jobs", json={"job_type": "simulate_failure", "payload": failure_payload()})
    client.post("/jobs", json={"job_type": "generate_report", "payload": {
        "customer_id": 1, "row_count": 1, "format": "csv"}})
    results = client.get("/jobs", params={"job_type": "generate_report", "limit": 1}).json()
    assert len(results) == 1
    assert results[0]["job_type"] == "generate_report"
