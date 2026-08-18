import asyncio

import httpx
import pytest
from sqlalchemy.orm import sessionmaker

from taskqueue.api import app
from taskqueue.database import get_session, init_db, make_engine
from taskqueue.handlers import HANDLERS
from taskqueue.worker import LeaseLostError, Worker


@pytest.mark.asyncio
async def test_two_http_workers_compete_and_complete_one_job(tmp_path):
    engine = make_engine(f"sqlite:///{(tmp_path / 'workers.db').as_posix()}")
    init_db(engine)
    factory = sessionmaker(engine, expire_on_commit=False)

    def sessions():
        with factory() as db:
            yield db

    app.dependency_overrides[get_session] = sessions
    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            created = (await client.post("/jobs", json={"job_type": "simulate_failure",
                "payload": {"fail_attempts": 0, "duration_seconds": 0}})).json()
        async def work(name):
            async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                return await Worker("http://test", name, lease_seconds=1).lease_once(client)
        outcomes = await asyncio.gather(work("worker-A"), work("worker-B"))
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            finished = (await client.get(f"/jobs/{created['id']}")).json()
        assert outcomes.count(True) == 1
        assert finished["state"] == "succeeded"
        assert finished["attempt_count"] == 1
    finally:
        app.dependency_overrides.clear()
        engine.dispose()


@pytest.mark.asyncio
async def test_graceful_stop_finishes_current_job_without_new_claim(monkeypatch):
    started = asyncio.Event()
    release = asyncio.Event()
    lease_calls = 0
    completed = False

    async def controlled_handler(_payload, _attempt):
        started.set()
        await release.wait()
        return {"ok": True}

    monkeypatch.setitem(HANDLERS, "simulate_failure", controlled_handler)
    job = {"id": "job-1", "job_type": "simulate_failure", "payload": {},
           "attempt_count": 1, "lease_token": "token-12345678"}

    def route(request):
        nonlocal lease_calls, completed
        if request.url.path == "/workers/lease":
            lease_calls += 1
            return httpx.Response(200, json=job if lease_calls == 1 else None)
        if request.url.path.endswith("/complete"):
            completed = True
            return httpx.Response(200, json={})
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(route)
    factory = lambda: httpx.AsyncClient(transport=transport, base_url="http://test")
    worker = Worker("http://test", "worker", lease_seconds=30, client_factory=factory)
    running = asyncio.create_task(worker.run())
    await started.wait()
    worker.stop()
    release.set()
    await asyncio.wait_for(running, 1)
    assert completed is True
    assert lease_calls == 1


@pytest.mark.asyncio
async def test_rejected_heartbeat_cancels_handler(monkeypatch):
    cancelled = asyncio.Event()

    async def long_handler(_payload, _attempt):
        try:
            await asyncio.sleep(10)
        finally:
            cancelled.set()

    monkeypatch.setitem(HANDLERS, "simulate_failure", long_handler)
    job = {"id": "job-1", "job_type": "simulate_failure", "payload": {},
           "attempt_count": 1, "lease_token": "token-12345678"}
    transport = httpx.MockTransport(lambda request: httpx.Response(
        409 if request.url.path.endswith("/heartbeat") else 200, json={}))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(LeaseLostError):
            await Worker("http://test", "worker", lease_seconds=0.06).execute(client, job)
    assert cancelled.is_set()
