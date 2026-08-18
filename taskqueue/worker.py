import asyncio
import logging
from collections.abc import Callable

import httpx

from .handlers import HANDLERS, PermanentJobError, RetryableJobError

log = logging.getLogger("taskqueue.worker")


class LeaseLostError(Exception):
    """Raised when the server no longer accepts this worker's active lease."""


class Worker:
    def __init__(
        self,
        server_url: str,
        name: str,
        poll_interval: float = 0.25,
        lease_seconds: float = 10,
        client_factory: Callable[[], httpx.AsyncClient] | None = None,
    ):
        self.server_url = server_url.rstrip("/")
        self.name = name
        self.poll_interval = poll_interval
        self.lease_seconds = lease_seconds
        self.stopping = False
        self._client_factory = client_factory or (lambda: httpx.AsyncClient(timeout=15))

    async def heartbeat(self, client: httpx.AsyncClient, job: dict) -> None:
        while True:
            await asyncio.sleep(self.lease_seconds / 3)
            response = await client.post(
                f"{self.server_url}/jobs/{job['id']}/heartbeat",
                json={"worker_id": self.name, "lease_token": job["lease_token"],
                      "lease_seconds": self.lease_seconds},
            )
            if response.status_code == 409:
                raise LeaseLostError("heartbeat rejected because the lease is no longer active")
            response.raise_for_status()

    async def _run_handler(self, client: httpx.AsyncClient, job: dict) -> dict:
        handler_task = asyncio.create_task(
            HANDLERS[job["job_type"]](job["payload"], job["attempt_count"])
        )
        heartbeat_task = asyncio.create_task(self.heartbeat(client, job))
        done, _ = await asyncio.wait(
            {handler_task, heartbeat_task}, return_when=asyncio.FIRST_COMPLETED
        )
        if heartbeat_task in done:
            handler_task.cancel()
            await asyncio.gather(handler_task, return_exceptions=True)
            await heartbeat_task
            raise LeaseLostError("heartbeat stopped unexpectedly")
        heartbeat_task.cancel()
        await asyncio.gather(heartbeat_task, return_exceptions=True)
        return await handler_task

    async def execute(self, client: httpx.AsyncClient, job: dict) -> None:
        token_prefix = job["lease_token"][:8]
        proof = {"worker_id": self.name, "lease_token": job["lease_token"]}
        log.info("executing job=%s worker=%s lease=%s attempt=%s state=leased",
                 job["id"], self.name, token_prefix, job["attempt_count"])
        try:
            result = await self._run_handler(client, job)
            response = await client.post(f"{self.server_url}/jobs/{job['id']}/complete",
                                         json={**proof, "result": result})
            if response.status_code == 409:
                raise LeaseLostError("completion rejected because the lease is no longer active")
            response.raise_for_status()
            log.info("completed job=%s worker=%s lease=%s state=succeeded",
                     job["id"], self.name, token_prefix)
        except (RetryableJobError, PermanentJobError) as exc:
            response = await client.post(f"{self.server_url}/jobs/{job['id']}/fail", json={
                **proof, "retryable": isinstance(exc, RetryableJobError), "error": str(exc)})
            if response.status_code == 409:
                raise LeaseLostError("failure report rejected because the lease is no longer active")
            response.raise_for_status()
            log.info("failed job=%s worker=%s lease=%s retryable=%s", job["id"],
                     self.name, token_prefix, isinstance(exc, RetryableJobError))

    async def lease_once(self, client: httpx.AsyncClient) -> bool:
        if self.stopping:
            return False
        response = await client.post(f"{self.server_url}/workers/lease", json={
            "worker_id": self.name, "supported_job_types": list(HANDLERS),
            "lease_seconds": self.lease_seconds})
        response.raise_for_status()
        job = response.json()
        if not job:
            return False
        await self.execute(client, job)
        return True

    async def run(self) -> None:
        async with self._client_factory() as client:
            while not self.stopping:
                try:
                    claimed = await self.lease_once(client)
                    if not claimed:
                        await asyncio.sleep(self.poll_interval)
                except LeaseLostError as exc:
                    log.warning("worker=%s lost lease: %s", self.name, exc)
                except httpx.HTTPError:
                    log.exception("worker=%s request failed", self.name)
                    await asyncio.sleep(self.poll_interval)

    def stop(self) -> None:
        """Stop polling after any currently executing job finishes."""
        self.stopping = True
