"""FastAPI process: HTTP endpoints that enqueue DBOS workflows.

The endpoint below never runs the workflow itself. It writes an ENQUEUED row to
the shared SQLite system database and returns immediately with a workflow id.
The worker process (workflow.py) owns the queue and executes the workflow.

Run it from the project root, in a second terminal (the worker must be up)::

    uv run python api.py

Then open http://127.0.0.1:8000/users/alice in a browser.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator
from urllib.parse import quote

import uvicorn
from dbos import DBOS, DBOSConfig, EnqueueOptions
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import HTMLResponse

# --- Shared workflow identity ------------------------------------------------
# Same names as in workflow.py: they are the contract between the processes.
WORKFLOW_APPLICATION_NAME = "user-service"  # the app that owns the queue
QUEUE_NAME = "user-queue"
WORKFLOW_NAME = "create_user"

# This API process is a DBOS application of its own: it only enqueues, so it
# must NOT use the worker's application name (that name owns the queue).
API_APPLICATION_NAME = "user-service-api"
# Version names must be unique per system database, so this app cannot reuse the
# worker's "0.1.0": DBOS rejects a version already owned by another application.
APP_VERSION = "0.1.0-api"

# --- SQLite storage ----------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
SYSTEM_DB_URL = os.getenv(
    "DBOS_SYSTEM_DATABASE_URL",
    f"sqlite:///{(BASE_DIR / 'dbos_system.sqlite').as_posix()}",
)


def _config() -> DBOSConfig:
    return {
        "name": API_APPLICATION_NAME,
        "application_version": APP_VERSION,
        # Distinct executor id, so this process never recovers worker workflows.
        "executor_id": "user-service-api",
        "system_database_url": SYSTEM_DB_URL,
        "log_level": os.getenv("DBOS_LOG_LEVEL", "INFO"),
    }


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    # Enqueueing goes through the system database, so DBOS must be initialized
    # and launched in this process too (it does not register any queue here).
    DBOS(config=_config())
    DBOS.launch()
    try:
        yield
    finally:
        DBOS.destroy()


app = FastAPI(title="DBOS queue demo", lifespan=lifespan)


def _enqueue_options() -> EnqueueOptions:
    """Tell DBOS which application/queue/workflow to target.

    ``application_name`` is what routes the row to the worker: the workflow is
    owned by that application, and only that application dequeues it.
    ``workflow_name`` is matched against the worker's registered function name.
    """
    return {
        "application_name": WORKFLOW_APPLICATION_NAME,
        "queue_name": QUEUE_NAME,
        "workflow_name": WORKFLOW_NAME,
    }


def _iso(epoch_ms: int | None) -> str | None:
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()


@app.get("/users/{user_id}", status_code=202)
async def create_user(
    user_id: str,
    response: Response,
    wait: bool = False,
) -> dict[str, Any]:
    """Enqueue the ``create_user`` workflow and return at once (202 Accepted).

    Add ``?wait=true`` to await the result through ``handle.get_result()``.
    """
    handle = await DBOS.enqueue_workflow_with_options_async(
        _enqueue_options(),
        user_id,
    )

    payload: dict[str, Any] = {
        "workflow_id": handle.workflow_id,
        "workflow": WORKFLOW_NAME,
        "application": WORKFLOW_APPLICATION_NAME,
        "queue": QUEUE_NAME,
        "status": "ENQUEUED",
        "status_url": f"/workflows/{quote(handle.workflow_id)}",
    }

    if wait:
        # Blocks this request until the worker finishes the workflow.
        payload["status"] = "SUCCESS"
        payload["result"] = await handle.get_result()
        response.status_code = 200

    return payload


@app.get("/workflows/{workflow_id}")
async def workflow_status(workflow_id: str) -> dict[str, Any]:
    """Read the durable status row the worker maintains in the system database."""
    status = await DBOS.get_workflow_status_async(workflow_id)
    if status is None:
        raise HTTPException(status_code=404, detail=f"Unknown workflow {workflow_id}")

    return {
        "workflow_id": status.workflow_id,
        "name": status.name,
        "status": status.status,
        "application": status.application_name,
        "queue": status.queue_name,
        "executor_id": status.executor_id,
        "app_version": status.app_version,
        "created_at": _iso(status.created_at),
        "completed_at": _iso(status.completed_at),
        "output": status.output,
        "error": str(status.error) if status.error else None,
    }


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return f"""<!doctype html>
<html lang="en">
  <head><meta charset="utf-8"><title>DBOS queue demo</title></head>
  <body style="font-family: system-ui, sans-serif; max-width: 42rem; margin: 3rem auto; line-height: 1.5">
    <h1>DBOS queue demo</h1>
    <p>
      Each request enqueues the <code>{WORKFLOW_NAME}</code> workflow on
      <code>{QUEUE_NAME}</code> (application <code>{WORKFLOW_APPLICATION_NAME}</code>)
      and returns immediately. The worker process executes it.
    </p>
    <ul>
      <li><a href="/users/alice">GET /users/alice</a> &mdash; enqueue, return at once</li>
      <li><a href="/users/bob?wait=true">GET /users/bob?wait=true</a> &mdash; enqueue and await the result</li>
      <li><a href="/docs">/docs</a> &mdash; OpenAPI UI</li>
    </ul>
    <p>Paste a <code>workflow_id</code> into <code>/workflows/&lt;id&gt;</code> to see its status.</p>
  </body>
</html>"""


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.getenv("PORT", "8000")))
