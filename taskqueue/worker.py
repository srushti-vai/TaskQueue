import asyncio
import logging

import httpx

from .handlers import HANDLERS, PermanentJobError, RetryableJobError

log = logging.getLogger("taskqueue.worker")


class Worker:
    def __init__(self, server_url: str, name: str, poll_interval: float = 0.25,
                 lease_seconds: float = 10):
        self.server_url = server_url.rstrip("/"); self.name = name
        self.poll_interval = poll_interval; self.lease_seconds = lease_seconds
        self.stopping = False

    async def heartbeat(self, client, job):
        while True:
            await asyncio.sleep(self.lease_seconds / 3)
            response = await client.post(f"{self.server_url}/jobs/{job['id']}/heartbeat",
                json={"worker_id": self.name, "lease_token": job["lease_token"]})
            response.raise_for_status()

    async def execute(self, client, job):
        proof = {"worker_id": self.name, "lease_token": job["lease_token"]}
        pulse = asyncio.create_task(self.heartbeat(client, job))
        try:
            result = await HANDLERS[job["job_type"]](job["payload"], job["attempt_count"])
            await client.post(f"{self.server_url}/jobs/{job['id']}/complete",
                              json={**proof, "result": result})
        except (RetryableJobError, PermanentJobError) as exc:
            await client.post(f"{self.server_url}/jobs/{job['id']}/fail",
                json={**proof, "retryable": isinstance(exc, RetryableJobError), "error": str(exc)})
        finally:
            pulse.cancel()
            await asyncio.gather(pulse, return_exceptions=True)

    async def run(self):
        async with httpx.AsyncClient(timeout=15) as client:
            while not self.stopping:
                try:
                    response = await client.post(f"{self.server_url}/workers/lease", json={
                        "worker_id": self.name, "supported_job_types": list(HANDLERS),
                        "lease_seconds": self.lease_seconds})
                    response.raise_for_status(); job = response.json()
                    if job: await self.execute(client, job)
                    else: await asyncio.sleep(self.poll_interval)
                except httpx.HTTPError:
                    log.exception("worker request failed"); await asyncio.sleep(self.poll_interval)

    def stop(self): self.stopping = True

