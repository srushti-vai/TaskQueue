import concurrent.futures
import time

from sqlalchemy.orm import sessionmaker

from taskqueue.database import init_db, make_engine
from taskqueue.handlers import webhook_signature
from taskqueue.models import JobState
from taskqueue.queue_service import lease, submit
from taskqueue.schemas import JobSubmit, LeaseRequest


def payload(): return {"fail_attempts": 0, "duration_seconds": 0}
def create(client, **extra):
    body = {"job_type": "simulate_failure", "payload": payload(), **extra}
    return client.post("/jobs", json=body)


def test_submit_validation_and_idempotency(client):
    first = create(client, idempotency_key="same")
    assert first.status_code == 200
    assert client.get(f"/jobs/{first.json()['id']}").json()["state"] == "queued"
    assert create(client, idempotency_key="same").json()["id"] == first.json()["id"]
    conflict = client.post("/jobs", json={"job_type": "simulate_failure",
        "payload": {"fail_attempts": 1, "duration_seconds": 0}, "idempotency_key": "same"})
    assert conflict.status_code == 409
    assert client.post("/jobs", json={"job_type": "unknown", "payload": {}}).status_code == 422
    assert client.post("/jobs", json={"job_type": "simulate_failure", "payload": {}}).status_code == 422


def test_success_cancel_retry_and_metrics(client):
    job = create(client).json(); leased = client.post("/workers/lease", json={
        "worker_id": "w", "supported_job_types": ["simulate_failure"], "lease_seconds": 2}).json()
    proof = {"worker_id": "w", "lease_token": leased["lease_token"]}
    assert client.post(f"/jobs/{job['id']}/heartbeat", json=proof).status_code == 200
    assert client.post(f"/jobs/{job['id']}/heartbeat", json={**proof, "worker_id": "x"}).status_code == 409
    assert client.post(f"/jobs/{job['id']}/complete", json={**proof, "result": {"ok": True}}).json()["state"] == "succeeded"
    assert client.post("/workers/lease", json={"worker_id": "w", "supported_job_types": ["simulate_failure"]}).json() is None
    waiting = create(client).json()
    assert client.post(f"/jobs/{waiting['id']}/cancel").json()["state"] == "cancelled"
    metrics = client.get("/metrics").json(); assert metrics["submitted_jobs"] == 2


def test_retry_failure_dead_letter_and_manual_retry(client):
    job = create(client, max_attempts=2).json()
    leased = client.post("/workers/lease", json={"worker_id": "w", "supported_job_types": ["simulate_failure"]}).json()
    proof = {"worker_id": "w", "lease_token": leased["lease_token"]}
    result = client.post(f"/jobs/{job['id']}/fail", json={**proof, "retryable": True, "error": "temporary"}).json()
    assert result["state"] == "retry_wait" and result["available_at"] > result["updated_at"]
    time.sleep(.12)
    leased = client.post("/workers/lease", json={"worker_id": "w", "supported_job_types": ["simulate_failure"]}).json()
    proof = {"worker_id": "w", "lease_token": leased["lease_token"]}
    result = client.post(f"/jobs/{job['id']}/fail", json={**proof, "retryable": True, "error": "again"}).json()
    assert result["state"] == "dead_letter"
    assert client.post(f"/jobs/{job['id']}/retry").json()["state"] == "queued"
    leased = client.post("/workers/lease", json={"worker_id": "w", "supported_job_types": ["simulate_failure"]}).json()
    proof = {"worker_id": "w", "lease_token": leased["lease_token"]}
    assert client.post(f"/jobs/{job['id']}/fail", json={**proof, "retryable": False, "error": "bad"}).json()["state"] == "failed"


def test_atomic_concurrent_leasing(tmp_path):
    engine = make_engine(f"sqlite:///{tmp_path / 'race.db'}"); init_db(engine)
    factory = sessionmaker(engine, expire_on_commit=False)
    with factory() as s: submit(s, JobSubmit(job_type="simulate_failure", payload=payload()))
    def claim(worker):
        with factory() as s:
            job = lease(s, LeaseRequest(worker_id=worker, supported_job_types=["simulate_failure"]))
            return job.id if job else None
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        claimed = list(pool.map(claim, [f"w{i}" for i in range(8)]))
    assert len([item for item in claimed if item]) == 1


def test_expiry_and_stale_fencing(client):
    job = create(client).json()
    a = client.post("/workers/lease", json={"worker_id": "A", "supported_job_types": ["simulate_failure"], "lease_seconds": .05}).json()
    time.sleep(.07)
    b = client.post("/workers/lease", json={"worker_id": "B", "supported_job_types": ["simulate_failure"], "lease_seconds": 1}).json()
    assert a["lease_token"] != b["lease_token"]
    stale = client.post(f"/jobs/{job['id']}/complete", json={"worker_id": "A", "lease_token": a["lease_token"], "result": {}})
    assert stale.status_code == 409
    ok = client.post(f"/jobs/{job['id']}/complete", json={"worker_id": "B", "lease_token": b["lease_token"], "result": {}})
    assert ok.json()["state"] == "succeeded"


def test_stable_webhook_signature():
    assert webhook_signature({"b": 2, "a": 1}, "delivery") == webhook_signature({"a": 1, "b": 2}, "delivery")

