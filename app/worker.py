"""Background task orchestration.

Each upload is processed by a daemon worker thread with its own SQLite
connection. Cancellation is cooperative: the cancel endpoint flips an event
and returns immediately, while the worker observes the event at every batch
boundary and aborts before anything can be published.

Finalization contract: a task may only enter its terminal state
('cancelled'/'failed') once its original uploads, staging directory and
parsed read records are all gone. When the staging directory cannot be
removed (permissions, transient IO errors, ...), the task is parked in
'cleanup_pending' with the booked terminal state kept in ``final_status``;
queries then show the task as pending cleanup and downloads stay closed.
A repeated cancel request or a service restart retries the finalization,
and only then does the task report its terminal state.
"""

from __future__ import annotations

import shutil
import threading
from pathlib import Path

from . import config, db
from .fastq import (
    FastqFormatError,
    TaskCancelled,
    parse_file,
    promote_results,
    publish,
    validate_pairing,
)

# task_id -> threading.Event set when the task is asked to cancel.
_cancel_events: dict[str, threading.Event] = {}
_registry_lock = threading.Lock()
# Keep worker threads reachable (mostly for tests / diagnostics).
_workers: dict[str, threading.Thread] = {}
# task_id -> thread retrying a blocked finalization (one per task).
_finalizers: dict[str, threading.Thread] = {}


class UploadTooLarge(Exception):
    def __init__(self, side: str, limit: int) -> None:
        self.side = side
        self.limit = limit
        super().__init__(f"{side} exceeds the {limit} byte upload limit")


class CleanupError(Exception):
    """Raised when a task's staging directory cannot be fully removed."""


def register_cancel_event(task_id: str) -> threading.Event:
    with _registry_lock:
        event = _cancel_events.get(task_id)
        if event is None:
            event = threading.Event()
            _cancel_events[task_id] = event
        return event


def request_cancel(task_id: str) -> None:
    register_cancel_event(task_id).set()


def task_dir(task_id: str) -> Path:
    return config.TASKS_DIR / task_id


def cleanup_task_dir(task_id: str) -> None:
    """Remove only this task's own directory; UUID names isolate tasks.

    Raises CleanupError when anything cannot be removed. Callers must not
    publish a terminal state in that case: the task is parked in
    'cleanup_pending' instead, so the leftover originals/staging are retried
    later instead of being silently leaked.
    """
    path = task_dir(task_id)
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass  # Already gone: nothing left to do.
    except OSError as exc:
        raise CleanupError(f"cannot remove staging directory {path}: {exc}") from exc


def finalize_task(task_id: str, *, cancelled: bool, message: str) -> bool:
    """Clean up after a stopped task, then publish its terminal state.

    The task's read records are deleted and its staging directory (original
    uploads, temp outputs) removed first; only when both succeed is the
    status moved to 'cancelled' or 'failed' and ``True`` returned. If the
    directory cannot be removed, the task is parked in 'cleanup_pending'
    with the booked terminal state in ``final_status`` and ``False`` is
    returned — a repeated cancel or a restart will resume the finalization.
    The original ``message`` (failure reason / cancellation note) is kept
    unchanged in both outcomes.
    """
    final = "cancelled" if cancelled else "failed"
    conn = db.connect()
    try:
        row = db.get_task(conn, task_id)
        if row is None or row["status"] in db.TERMINAL_STATUSES:
            # Nothing to do, and never touch a task that already reached its
            # terminal state (in particular published results of a succeeded
            # task must survive).
            return True
        db.delete_records(conn, task_id)
        try:
            cleanup_task_dir(task_id)
        except CleanupError:
            db.update_task(
                conn,
                task_id,
                status=db.STATUS_CLEANUP_PENDING,
                stage=config.STAGE_CLEANUP,
                message=message,
                final_status=final,
            )
            return False
        db.update_task(
            conn,
            task_id,
            status=final,
            stage=config.STAGE_DONE,
            progress=1.0 if cancelled else 0.0,
            message=message,
            result_r1="",
            result_r2="",
            final_status="",
        )
        return True
    finally:
        conn.close()
        with _registry_lock:
            _cancel_events.pop(task_id, None)


def retry_finalize(task_id: str) -> None:
    """Resume a blocked finalization in the background.

    Used when a cancel request arrives for a 'cleanup_pending' task: the
    request is accepted immediately and this thread re-attempts the cleanup
    so the task can reach its booked terminal state.
    """
    with _registry_lock:
        existing = _finalizers.get(task_id)
        if existing is not None and existing.is_alive():
            return
        thread = threading.Thread(
            target=_retry_finalize,
            args=(task_id,),
            name=f"fastq-finalize-{task_id[:8]}",
            daemon=True,
        )
        _finalizers[task_id] = thread
        thread.start()


def _retry_finalize(task_id: str) -> None:
    try:
        conn = db.connect()
        try:
            row = db.get_task(conn, task_id)
        finally:
            conn.close()
        if row is None or row["status"] != db.STATUS_CLEANUP_PENDING:
            return
        finalize_task(
            task_id,
            cancelled=row["final_status"] == "cancelled",
            message=row["message"],
        )
    finally:
        with _registry_lock:
            _finalizers.pop(task_id, None)


def _cancel_requested(conn, task_id: str, event: threading.Event) -> bool:
    """True once a cancel has been accepted for this task.

    The in-memory event is the fast path; the database status closes the
    race in which the cancel endpoint has already flipped the row to
    'cancelling' but has not set the event yet — an accepted cancel must
    always lead to the 'cancelled' terminal state.
    """
    if event.is_set():
        return True
    row = db.get_task(conn, task_id)
    return row is not None and row["status"] == "cancelling"


def run_task(task_id: str) -> None:
    """Worker entry point: parse, validate, pair, publish or fail atomically."""
    event = register_cancel_event(task_id)
    conn = db.connect()
    base = task_dir(task_id)
    r1_path = base / "input_R1.fastq"
    r2_path = base / "input_R2.fastq"
    total_bytes = max(r1_path.stat().st_size + r2_path.stat().st_size, 1)

    def parse_progress(side: int, byte_offset: int) -> None:
        other = 0 if side == 1 else r1_path.stat().st_size
        current = other + byte_offset
        db.update_task(
            conn,
            task_id,
            stage=config.STAGE_PARSE,
            progress=min(0.9 * current / total_bytes, 0.9),
        )

    try:
        if event.is_set():
            raise TaskCancelled()
        db.update_task(conn, task_id, status="processing", stage=config.STAGE_PARSE)

        n1 = parse_file(
            conn, task_id, r1_path, 1, event.is_set,
            lambda off: parse_progress(1, off),
        )
        n2 = parse_file(
            conn, task_id, r2_path, 2, event.is_set,
            lambda off: parse_progress(2, off),
        )
        if event.is_set():
            raise TaskCancelled()

        db.update_task(conn, task_id, stage=config.STAGE_VALIDATE, progress=0.92)
        validate_pairing(conn, task_id)
        if n1 != n2:
            # Defensive: orphan validation should already have caught this.
            raise FastqFormatError(
                f"record count mismatch: R1 has {n1} records, R2 has {n2}"
            )
        if event.is_set():
            raise TaskCancelled()

        db.update_task(conn, task_id, stage=config.STAGE_PUBLISH, progress=0.95)
        out_dir = base / "output"
        tmp1, tmp2 = publish(
            conn,
            task_id,
            out_dir,
            event.is_set,
            lambda fraction: db.update_task(
                conn, task_id, stage=config.STAGE_PUBLISH,
                progress=0.95 + 0.05 * fraction,
            ),
        )

        # Arbitrate success vs. cancellation in the database BEFORE any result
        # file gets its final name. If the cancel endpoint flipped the status
        # to 'cancelling', this UPDATE matches no row: we keep the temp files
        # unpublished and fall through to cancellation cleanup.
        cur = conn.execute(
            "UPDATE tasks SET status = 'succeeded', stage = ?, progress = 1.0, "
            "message = ?, result_r1 = ?, result_r2 = ?, updated_at = ? "
            "WHERE id = ? AND status != 'cancelling'",
            (
                config.STAGE_DONE,
                f"paired {n1} reads",
                "output/R1.fastq",
                "output/R2.fastq",
                db.utcnow(),
                task_id,
            ),
        )
        if cur.rowcount == 0:
            raise TaskCancelled()

        # We have won the arbitration; the temp files are complete and durable.
        promote_results(out_dir, tmp1, tmp2)
        db.delete_records(conn, task_id)
        # Inputs are no longer needed once results are durable.
        for stale in (r1_path, r2_path):
            try:
                stale.unlink(missing_ok=True)
            except OSError:
                pass
    except TaskCancelled:
        finalize_task(
            task_id, cancelled=True, message="task cancelled before completion"
        )
    except FastqFormatError as exc:
        if _cancel_requested(conn, task_id, event):
            finalize_task(
                task_id, cancelled=True, message="task cancelled before completion"
            )
        else:
            finalize_task(task_id, cancelled=False, message=f"rejected: {exc}")
    except Exception as exc:  # noqa: BLE001 - any failure fails the whole task
        if _cancel_requested(conn, task_id, event):
            finalize_task(
                task_id, cancelled=True, message="task cancelled before completion"
            )
        else:
            finalize_task(task_id, cancelled=False, message=f"unexpected error: {exc!r}")
    finally:
        conn.close()
        with _registry_lock:
            _workers.pop(task_id, None)


def start_worker(task_id: str) -> None:
    thread = threading.Thread(
        target=run_task, args=(task_id,), name=f"fastq-task-{task_id[:8]}", daemon=True
    )
    with _registry_lock:
        _workers[task_id] = thread
    thread.start()


def recover_on_startup() -> dict[str, int]:
    """Resume finalization for every task that is not in a terminal state.

    - 'cleanup_pending' tasks (cleanup was blocked before the restart) retry
      their cleanup and then report the terminal state booked in
      ``final_status``, keeping the original message;
    - 'cancelling' tasks (cancel was accepted) finish as 'cancelled';
    - 'pending'/'processing' tasks were interrupted mid-flight and finish as
      'failed' once their staging is gone.

    If a staging directory still cannot be removed, the task stays in
    'cleanup_pending' and the next cancel request or restart retries again.
    Succeeded tasks keep their result files and remain downloadable.
    """
    conn = db.connect()
    try:
        rows = db.non_terminal_tasks(conn)
    finally:
        conn.close()
    finalized = 0
    still_pending = 0
    for row in rows:
        status = row["status"]
        if status == db.STATUS_CLEANUP_PENDING:
            cancelled = row["final_status"] == "cancelled"
            message = row["message"]
        elif status == "cancelling":
            cancelled = True
            message = "task cancelled before completion"
        else:
            cancelled = False
            message = "interrupted by service restart"
        try:
            done = finalize_task(row["id"], cancelled=cancelled, message=message)
        except Exception:  # noqa: BLE001 - one bad task must not block others
            done = False
        if done:
            finalized += 1
        else:
            still_pending += 1
    # Cancellation registry is empty in a fresh process.
    return {"finalized": finalized, "cleanup_pending": still_pending}
