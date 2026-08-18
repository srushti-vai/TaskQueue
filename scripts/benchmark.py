"""Local SQLite service benchmark; writes only measured results."""
import asyncio
import json
import statistics
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import sessionmaker

from taskqueue.database import init_db, make_engine
from taskqueue.handlers import HANDLERS
from taskqueue.models import Job, JobState
from taskqueue.queue_service import complete, lease, submit
from taskqueue.schemas import JobSubmit, LeaseRequest


def run(count=500, workers=1):
    with tempfile.TemporaryDirectory() as directory:
        database = (Path(directory) / "bench.db").as_posix()
        engine = make_engine(f"sqlite:///{database}"); init_db(engine)
        factory = sessionmaker(engine, expire_on_commit=False); starts = {}
        before = time.perf_counter()
        with factory() as db:
            for i in range(count):
                job = submit(db, JobSubmit(job_type="simulate_failure", payload={"fail_attempts": 0, "duration_seconds": 0}, idempotency_key=f"b-{i}")); starts[job.id] = time.perf_counter()
        time.sleep(.01)
        latencies = []
        active_job_ids = set()
        active_lock = threading.Lock()
        duplicate_active_lease_violations = 0

        def work(name):
            nonlocal duplicate_active_lease_violations
            while True:
                with factory() as db:
                    job = lease(db, LeaseRequest(worker_id=name, supported_job_types=["simulate_failure"]))
                    if not job: return
                    with active_lock:
                        if job.id in active_job_ids:
                            duplicate_active_lease_violations += 1
                        active_job_ids.add(job.id)
                    try:
                        result = asyncio.run(HANDLERS[job.job_type](job.payload, job.attempt_count))
                        complete(db, job, name, job.lease_token, result)
                        latencies.append(time.perf_counter() - starts[job.id])
                    finally:
                        with active_lock:
                            active_job_ids.discard(job.id)
        with ThreadPoolExecutor(max_workers=workers) as pool: list(pool.map(work, [f"w{i}" for i in range(workers)]))
        elapsed = time.perf_counter() - before; ordered = sorted(latencies)
        with factory() as db:
            total_completed = db.scalar(select(func.count()).select_from(Job).where(
                Job.state == JobState.SUCCEEDED))
            retry_count = db.scalar(select(func.sum(Job.retry_count))) or 0
        result = {"workers": workers, "total_completed": total_completed,
            "elapsed_seconds": elapsed, "jobs_per_second": total_completed / elapsed,
            "p50_latency_seconds": statistics.median(ordered),
            "p95_latency_seconds": ordered[int(.95 * (len(ordered)-1))],
            "retry_count": retry_count,
            "duplicate_active_lease_violations": duplicate_active_lease_violations}
        engine.dispose()
        return result


def main():
    results = [run(workers=w) for w in (1, 2, 4)]
    output = Path("reports"); output.mkdir(exist_ok=True)
    output.joinpath("benchmark.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
    lines = ["# Local SQLite benchmark", "", "Measured on this machine; not a production-scale claim.", "", "| Workers | Completed | Jobs/s | p50 (s) | p95 (s) | Lease violations |", "|---:|---:|---:|---:|---:|---:|"]
    for r in results: lines.append(f"| {r['workers']} | {r['total_completed']} | {r['jobs_per_second']:.2f} | {r['p50_latency_seconds']:.4f} | {r['p95_latency_seconds']:.4f} | {r['duplicate_active_lease_violations']} |")
    output.joinpath("benchmark.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(results, indent=2))


if __name__ == "__main__": main()
