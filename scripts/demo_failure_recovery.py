"""Deterministic in-process lease recovery demonstration."""
import tempfile
import time
from pathlib import Path

from sqlalchemy.orm import sessionmaker

from taskqueue.database import init_db, make_engine
from taskqueue.queue_service import InvalidLease, complete, lease, submit
from taskqueue.schemas import JobSubmit, LeaseRequest


def main():
    with tempfile.TemporaryDirectory() as directory:
        engine = make_engine(f"sqlite:///{Path(directory) / 'demo.db'}"); init_db(engine)
        sessions = sessionmaker(engine, expire_on_commit=False)
        key = "failure-recovery-demo"
        with sessions() as db:
            job = submit(db, JobSubmit(job_type="simulate_failure", payload={"fail_attempts": 0, "duration_seconds": 1}, idempotency_key=key))
            print(f"Submitted {job.id}")
            a = lease(db, LeaseRequest(worker_id="worker-A", supported_job_types=[job.job_type], lease_seconds=.2))
            print(f"Worker A leased token {a.lease_token[:8]} then lost its heartbeat")
        time.sleep(.25)
        with sessions() as db:
            b = lease(db, LeaseRequest(worker_id="worker-B", supported_job_types=["simulate_failure"], lease_seconds=2))
            print(f"Worker B reclaimed token {b.lease_token[:8]}")
            try: complete(db, db.get(type(b), b.id), "worker-A", a.lease_token, {})
            except InvalidLease: print("Stale Worker A completion rejected")
            complete(db, db.get(type(b), b.id), "worker-B", b.lease_token, {"ok": True})
            duplicate = submit(db, JobSubmit(job_type="simulate_failure", payload={"fail_attempts": 0, "duration_seconds": 1}, idempotency_key=key))
            print(f"Worker B completed; idempotent resubmit returned same job: {duplicate.id == b.id}")


if __name__ == "__main__": main()

