"""DBOS worker process: the queued workflow and its steps.

This is the process that *executes* work. It registers the queue, so DBOS knows
this application owns it, and then dequeues and runs ``create_user`` workflows
that someone else (api.py, or any DBOS client) put into that queue.

Run it from the project root, in its own terminal::

    uv run python workflow.py
"""

from __future__ import annotations

import asyncio
import os
import signal
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dbos import DBOS, DBOSConfig

# --- Workflow identity -------------------------------------------------------
# api.py enqueues by these exact names, so they are part of the contract
# between the two processes.
APPLICATION_NAME = "user-service"  # owner of the queue and of the workflow
QUEUE_NAME = "user-queue"
WORKFLOW_NAME = "create_user"  # must match the function name below

APP_VERSION = "0.1.0"

# --- SQLite storage ----------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent

# DBOS system database (workflow/step checkpoints, queue state, statuses).
SYSTEM_DB_URL = os.getenv(
    "DBOS_SYSTEM_DATABASE_URL",
    f"sqlite:///{(BASE_DIR / 'dbos_system.sqlite').as_posix()}",
)

# The application's own data, kept in a separate SQLite file.
USERS_DB_PATH = Path(os.getenv("USERS_DB_PATH", str(BASE_DIR / "users.sqlite")))

# How long the fake email call takes. Raise it to crash the worker while a
# workflow is in flight and watch DBOS recover it on restart.
EMAIL_DELAY_SECONDS = float(os.getenv("EMAIL_DELAY_SECONDS", "2"))


# --- Steps -------------------------------------------------------------------
# Every step is checkpointed in the system database. If the process crashes
# mid-workflow, completed steps are not repeated after restart.


@DBOS.step()
async def normalize_user_id(user_id: str) -> str:
    """Step 1: validate/normalize the id (pretend it calls an external service)."""
    normalized = user_id.strip().lower()
    if not normalized:
        raise ValueError("user_id must not be empty")

    await asyncio.sleep(0.5)
    print("NORMALIZE USER ID")
    return normalized


@DBOS.step()
async def save_user(user_id: str) -> dict[str, Any]:
    """Step 2: persist the user in the application's SQLite database."""
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    conn = sqlite3.connect(USERS_DB_PATH, timeout=30)
    try:
        with conn:  # one transaction, committed on success
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id    TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO users (user_id, created_at) VALUES (?, ?)
                ON CONFLICT(user_id) DO NOTHING
                """,
                (user_id, created_at),
            )
        row = conn.execute(
            "SELECT user_id, created_at FROM users WHERE user_id = ?", (user_id,)
        ).fetchone()
    finally:
        conn.close()

    user = {"user_id": row[0], "created_at": row[1]}
    print("SAVE USER")
    return user


@DBOS.step()
async def send_welcome_email(user_id: str) -> str:
    """Step 3: pretend to call a slow external API."""
    await asyncio.sleep(EMAIL_DELAY_SECONDS)
    print("SEND WELCOME EMAIL")
    return f"Welcome email sent to {user_id}"


# --- Workflow ----------------------------------------------------------------


@DBOS.workflow()
async def create_user(user_id: str) -> dict[str, Any]:
    """The workflow that api.py enqueues: three durable steps in a row."""
    normalized = await normalize_user_id(user_id)
    # A plain delay, not a checkpointed one: DBOS.sleep_async(10) would be durable.
    await asyncio.sleep(10)
    user = await save_user(normalized)
    confirmation = await send_welcome_email(normalized)
    return {**user, "confirmation": confirmation}


# --- Process -----------------------------------------------------------------


def _config() -> DBOSConfig:
    return {
        "name": APPLICATION_NAME,
        "application_version": APP_VERSION,
        # A distinct executor id per process keeps recovery state unambiguous.
        "executor_id": "user-service-worker",
        "system_database_url": SYSTEM_DB_URL,
        "log_level": os.getenv("DBOS_LOG_LEVEL", "INFO"),
    }


def _install_stop_handlers(stop: threading.Event) -> None:
    """First Ctrl+C drains in-flight workflows; the second one exits at once.

    Interrupting is safe: every completed step is checkpointed, so a workflow cut
    short stays in the system database and DBOS recovers it on the next launch.
    """
    pressed = False

    def _stop(signum: int, frame: Any) -> None:
        nonlocal pressed
        if pressed:
            print(
                "Second interrupt - exiting now. "
                "Interrupted workflows will be recovered on the next start.",
                flush=True,
            )
            os._exit(130)
        pressed = True
        drain = os.getenv("SHUTDOWN_TIMEOUT_SEC", "5")
        print(
            f"Stopping worker: waiting up to {drain}s for in-flight workflows "
            "(press Ctrl+C again to skip the wait)...",
            flush=True,
        )
        stop.set()

    signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):  # Ctrl+Break on Windows
        signals.append(signal.SIGBREAK)
    for sig in signals:
        try:
            signal.signal(sig, _stop)
        except (ValueError, OSError):  # not the main thread / unsupported signal
            pass


def main() -> None:
    DBOS(config=_config())
    DBOS.launch()

    # Registered after launch, so this process owns the queue: only the owner
    # dequeues from it. worker_concurrency=2 lets two workflows run at once.
    DBOS.register_queue(QUEUE_NAME, worker_concurrency=2)

    print(
        f"Worker ready: application={APPLICATION_NAME} queue={QUEUE_NAME} "
        f"workflow={WORKFLOW_NAME} system_db={SYSTEM_DB_URL}",
        flush=True,
    )

    stop = threading.Event()
    _install_stop_handlers(stop)
    try:
        while not stop.wait(0.5):
            pass
    finally:
        # How long to let active workflows finish before dropping them; anything
        # still running is recovered on the next launch.
        drain_sec = int(float(os.getenv("SHUTDOWN_TIMEOUT_SEC", "5")))
        DBOS.destroy(workflow_completion_timeout_sec=drain_sec)


if __name__ == "__main__":
    main()
