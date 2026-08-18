import asyncio
import json
import signal
from pathlib import Path

import httpx
import typer
import uvicorn

from .worker import Worker

app = typer.Typer(help="TaskQueue command-line interface")


def print_json(value) -> None:
    typer.echo(json.dumps(value, indent=2))


@app.command()
def server(host: str = "127.0.0.1", port: int = 8000): uvicorn.run("taskqueue.api:app", host=host, port=port)


@app.command()
def worker(name: str = "worker-1", server_url: str = "http://127.0.0.1:8000"):
    instance = Worker(server_url, name)
    handled = [signal.SIGINT]
    if hasattr(signal, "SIGTERM"):
        handled.append(signal.SIGTERM)
    previous = {item: signal.signal(item, lambda *_: instance.stop()) for item in handled}
    try:
        asyncio.run(instance.run())
    finally:
        for item, handler in previous.items():
            signal.signal(item, handler)


@app.command()
def submit(job_type: str, payload: Path, idempotency_key: str | None = None,
           server_url: str = "http://127.0.0.1:8000"):
    data = json.loads(payload.read_text(encoding="utf-8"))
    print_json(httpx.post(f"{server_url}/jobs", json={"job_type": job_type, "payload": data,
        "idempotency_key": idempotency_key}).raise_for_status().json())


@app.command()
def status(job_id: str, server_url: str = "http://127.0.0.1:8000"):
    print_json(httpx.get(f"{server_url}/jobs/{job_id}").raise_for_status().json())


@app.command("list")
def list_command(state: str | None = None, server_url: str = "http://127.0.0.1:8000"):
    print_json(httpx.get(f"{server_url}/jobs", params={"state": state} if state else {})
               .raise_for_status().json())


@app.command()
def cancel(job_id: str, server_url: str = "http://127.0.0.1:8000"):
    print_json(httpx.post(f"{server_url}/jobs/{job_id}/cancel").raise_for_status().json())


@app.command("retry")
def retry_command(job_id: str, server_url: str = "http://127.0.0.1:8000"):
    print_json(httpx.post(f"{server_url}/jobs/{job_id}/retry").raise_for_status().json())
