"""Tests for the cleanup-pending finalization contract.

A task may only enter its terminal state ('cancelled'/'failed') once its
original uploads, staging directory and parsed read records are all gone.
When the staging directory cannot be removed, the task is parked in
'cleanup_pending' (queries show it as pending cleanup, downloads stay
closed); a repeated cancel or a service restart resumes the finalization.

Run with the project virtualenv:
    .venv/bin/python -I tests/test_cleanup.py
"""

from __future__ import annotations

import asyncio
import io
import os
import shutil
import sys
import tempfile
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import httpx  # noqa: E402

from app import config, db, worker  # noqa: E402
from app.main import app  # noqa: E402

RESULTS: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f" - {detail}" if detail else ""))


def make_record(rid: str, seq: bytes, quality: bytes | None = None,
                suffix: str = "") -> bytes:
    if quality is None:
        quality = b"~" * len(seq)
    return b"\n".join((b"@" + rid.encode() + suffix.encode(), seq, b"+", quality)) + b"\n"


def fastq(records: list[bytes]) -> io.BytesIO:
    return io.BytesIO(b"".join(records))


async def upload(client: httpx.AsyncClient, r1: io.BytesIO,
                 r2: io.BytesIO) -> httpx.Response:
    r1.seek(0)
    r2.seek(0)
    return await client.post(
        "/tasks",
        files={"r1": ("r1.fastq", r1, "application/octet-stream"),
               "r2": ("r2.fastq", r2, "application/octet-stream")},
    )


async def wait_for(client: httpx.AsyncClient, task_id: str, want: str,
                   timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = (await client.get(f"/tasks/{task_id}")).json()
        if body["status"] == want:
            return body
        await asyncio.sleep(0.05)
    raise AssertionError(f"task {task_id} never reached status {want!r}")


async def wait_terminal(client: httpx.AsyncClient, task_id: str,
                        timeout: float = 60.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        body = (await client.get(f"/tasks/{task_id}")).json()
        if body["status"] in ("succeeded", "failed", "cancelled"):
            return body
        await asyncio.sleep(0.05)
    raise AssertionError(f"task {task_id} did not finish in {timeout}s")


def record_count(task_id: str) -> int:
    conn = db.connect()
    try:
        return conn.execute(
            "SELECT COUNT(*) AS n FROM records WHERE task_id = ?", (task_id,)
        ).fetchone()["n"]
    finally:
        conn.close()


class block_cleanup:
    """Make worker.cleanup_task_dir fail, simulating a locked staging dir."""

    def __enter__(self):
        self.original = worker.cleanup_task_dir

        def blocked(task_id: str) -> None:
            raise worker.CleanupError("simulated locked staging directory")

        worker.cleanup_task_dir = blocked
        return self

    def __exit__(self, *exc) -> None:
        worker.cleanup_task_dir = self.original


async def main_async() -> int:
    tmp_root = Path(tempfile.mkdtemp(prefix="fastq-cleanup-",
                                     dir=str(ROOT / "tests" / "output")))
    data_dir = tmp_root / "data"
    os.environ["FASTQ_DATA_DIR"] = str(data_dir)
    config.DATA_DIR = data_dir
    config.DB_PATH = data_dir / "tasks.db"
    config.TASKS_DIR = data_dir / "tasks"

    db._initialized = False
    db.init_db()
    worker.recover_on_startup()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test",
                                 timeout=60) as client:
        # A succeeded task used later to prove cleanup never touches it.
        good_r1 = fastq([make_record("g1", b"AAAA", suffix="/1"),
                         make_record("g2", b"CCCC", suffix="/1")])
        good_r2 = fastq([make_record("g2", b"cccc", suffix="/2"),
                         make_record("g1", b"aaaa", suffix="/2")])
        r = await upload(client, good_r1, good_r2)
        good_id = (await wait_terminal(client, r.json()["id"]))["id"]
        good_dir = config.TASKS_DIR / good_id
        record("setup: reference task succeeded",
               (await client.get(f"/tasks/{good_id}")).json()["status"] == "succeeded")

        # ---- 1. malformed FASTQ + blocked cleanup -> cleanup_pending,
        #         repeated cancel finishes it as 'failed' with the original
        #         failure reason preserved.
        bad_r2 = fastq([make_record("x", b"AAAA", b"~~~")])  # seq 4 != qual 3
        with block_cleanup():
            r = await upload(client, fastq([make_record("x", b"AAAA")]), bad_r2)
            tid = r.json()["id"]
            body = await wait_for(client, tid, "cleanup_pending")
            record("blocked cleanup -> cleanup_pending (待清理)",
                   body["status"] == "cleanup_pending")
            record("pending task keeps original failure reason",
                   "length" in body["message"] and "rejected" in body["message"],
                   body["message"])
            record("pending task booked terminal state failed",
                   body["final_status"] == "failed", str(body["final_status"]))
            record("pending task stage is cleanup", body["stage"] == "cleanup",
                   body["stage"])
            gd = await client.get(f"/tasks/{tid}/download/r1")
            record("pending task results not downloadable (409)",
                   gd.status_code == 409, str(gd.status_code))
            staging = config.TASKS_DIR / tid
            originals = [(staging / name).is_file()
                         for name in ("input_R1.fastq", "input_R2.fastq")]
            record("originals retained while cleanup is blocked",
                   staging.is_dir() and all(originals))
            record("read records purged while cleanup is blocked",
                   record_count(tid) == 0, str(record_count(tid)))

            # A repeated cancel while still blocked is accepted but cannot
            # finish: the task stays cleanup_pending.
            cr = await client.post(f"/tasks/{tid}/cancel")
            record("repeated cancel accepted while blocked",
                   cr.status_code == 200, str(cr.status_code))
            await asyncio.sleep(0.3)  # let the background retry fail again
            still = (await client.get(f"/tasks/{tid}")).json()
            record("still cleanup_pending while dir locked",
                   still["status"] == "cleanup_pending", still["status"])

            # The blocked task must not degrade the service: a new task
            # still pairs and publishes normally.
            r = await upload(client, fastq([make_record("n1", b"TTTT")]),
                             fastq([make_record("n1", b"tttt")]))
            new_body = await wait_terminal(client, r.json()["id"])
            new_id = new_body["id"]
            record("new task succeeds while another is cleanup_pending",
                   new_body["status"] == "succeeded", new_body["status"])

        # Directory "becomes deletable" again: repeated cancel resumes the
        # finalization and the task reaches its booked terminal state.
        cr = await client.post(f"/tasks/{tid}/cancel")
        record("cancel after recovery accepted", cr.status_code == 200,
               str(cr.status_code))
        body = await wait_terminal(client, tid)
        record("finalized task reports failed (not cancelled)",
               body["status"] == "failed", body["status"])
        record("final failure keeps the original reason",
               "length" in body["message"] and "rejected" in body["message"],
               body["message"])
        record("final_status cleared once terminal",
               body["final_status"] is None, str(body["final_status"]))
        record("staging removed after finalization",
               not (config.TASKS_DIR / tid).exists())
        record("reference task results still downloadable",
               (await client.get(f"/tasks/{good_id}/download/r1")).status_code == 200
               and (good_dir / "output" / "R1.fastq").is_file())

        # ---- 2. cancelled task + blocked cleanup -> cleanup_pending with
        #         final_status cancelled; a restart finishes it as cancelled.
        n = 50000
        ids = [f"read-{i}" for i in range(n)]
        big_r1 = fastq([make_record(rid, b"A" * 40) for rid in ids])
        big_r2 = fastq([make_record(rid, b"a" * 40) for rid in reversed(ids)])
        with block_cleanup():
            r = await upload(client, big_r1, big_r2)
            cancel_id = r.json()["id"]
            cr = await client.post(f"/tasks/{cancel_id}/cancel")
            record("cancel accepted promptly", cr.status_code == 200,
                   str(cr.status_code))
            body = await wait_for(client, cancel_id, "cleanup_pending")
            record("cancelled task with blocked cleanup -> cleanup_pending",
                   body["status"] == "cleanup_pending")
            record("pending cancel booked terminal state cancelled",
                   body["final_status"] == "cancelled", str(body["final_status"]))
            record("cancelled-originals retained while blocked",
                   (config.TASKS_DIR / cancel_id / "input_R1.fastq").is_file())
        # Restart with the directory deletable again: recovery must finish
        # the approved cancellation and remove the staging.
        worker.recover_on_startup()
        body = (await client.get(f"/tasks/{cancel_id}")).json()
        record("approved cancel eventually reports cancelled after restart",
               body["status"] == "cancelled", body["status"])
        record("cancelled staging removed after restart recovery",
               not (config.TASKS_DIR / cancel_id).exists())

        # ---- 3. restart with a genuinely undeletable directory (chmod):
        #         interrupted task -> cleanup_pending; after chmod is fixed,
        #         a repeated cancel finishes it as failed.
        stuck_id = uuid.uuid4().hex
        base = config.TASKS_DIR / stuck_id
        base.mkdir(parents=True)
        (base / "input_R1.fastq").write_bytes(b"@x\nAAAA\n+\n~~~~\n")
        (base / "input_R2.fastq").write_bytes(b"@x\nAAAA\n+\n~~~~\n")
        conn = db.connect()
        db.create_task(conn, stuck_id, "r1.fastq", "r2.fastq", 14, 14)
        db.update_task(conn, stuck_id, status="processing",
                       stage=config.STAGE_PARSE)
        conn.close()
        os.chmod(base, 0o500)  # not writable -> rmtree fails with EACCES
        try:
            worker.recover_on_startup()
            body = (await client.get(f"/tasks/{stuck_id}")).json()
            record("restart with locked dir -> cleanup_pending",
                   body["status"] == "cleanup_pending", body["status"])
            record("locked task booked failed with restart reason",
                   body["final_status"] == "failed"
                   and body["message"] == "interrupted by service restart",
                   f"{body['final_status']} / {body['message']}")
            record("originals survive while dir is locked",
                   (base / "input_R1.fastq").is_file()
                   and (base / "input_R2.fastq").is_file())
        finally:
            os.chmod(base, 0o700)
        cr = await client.post(f"/tasks/{stuck_id}/cancel")
        record("repeated cancel after unlock accepted", cr.status_code == 200,
               str(cr.status_code))
        body = await wait_terminal(client, stuck_id)
        record("unlocked task finalizes as failed",
               body["status"] == "failed", body["status"])
        record("restart reason preserved after finalization",
               body["message"] == "interrupted by service restart",
               body["message"])
        record("unlocked staging removed", not base.exists())

        # ---- 4. restart recovery finishes a task whose cancel was approved
        #         but which never got finalized (status 'cancelling').
        cancelling_id = uuid.uuid4().hex
        cdir = config.TASKS_DIR / cancelling_id
        cdir.mkdir(parents=True)
        (cdir / "input_R1.fastq").write_bytes(b"@y\nCCCC\n+\n~~~~\n")
        conn = db.connect()
        db.create_task(conn, cancelling_id, "r1.fastq", "r2.fastq", 12, 12)
        db.update_task(conn, cancelling_id, status="cancelling")
        conn.close()
        worker.recover_on_startup()
        body = (await client.get(f"/tasks/{cancelling_id}")).json()
        record("restart finishes approved cancel as cancelled",
               body["status"] == "cancelled", body["status"])
        record("cancelling staging removed on restart", not cdir.exists())

        # ---- 5. cleanup never touches other tasks or successful results.
        record("reference task still succeeded at the end",
               (await client.get(f"/tasks/{good_id}")).json()["status"] == "succeeded")
        dl = await client.get(f"/tasks/{good_id}/download/r2")
        expected_r2 = (make_record("g1", b"aaaa", suffix="/2")
                       + make_record("g2", b"cccc", suffix="/2"))
        record("reference R2 bytes intact",
               dl.status_code == 200 and dl.content == expected_r2)
        # Only directories of the two still-valid succeeded tasks may remain.
        leftover = sorted(p.name for p in config.TASKS_DIR.glob("*"))
        record("no orphan staging dirs left behind",
               leftover == sorted([good_id, new_id]),
               f"leftover dirs: {leftover}")

    shutil.rmtree(tmp_root, ignore_errors=True)
    failed = [name for name, ok, _ in RESULTS if not ok]
    print("\n%d/%d checks passed" % (len(RESULTS) - len(failed), len(RESULTS)))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))
