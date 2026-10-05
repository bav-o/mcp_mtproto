"""Выгрузка: уникальность, лимиты частей, атомарная публикация, FloodWait и продолжение,
граница since (ревью, пп. 5, 8, 11, 12, 14)."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from telethon.errors import FloodWaitError

import app.export as e
import app.server as server
from app.common import ToolFailure
from conftest import DATE, HUMAN, message


class HistoryClient:
    """Эмулирует iter_messages(reverse=True) с исключающими min_id / offset_date, как в Telethon."""

    def __init__(self, msgs, flood_after=None, flood_seconds=120):
        self.msgs = msgs
        self.flood_after, self.flood_seconds = flood_after, flood_seconds
        self.calls = []

    async def iter_messages(self, entity, reverse=True, min_id=None, offset_date=None):
        assert reverse
        self.calls.append({"min_id": min_id, "offset_date": offset_date})
        for i, m in enumerate(self.msgs):
            if min_id and m.id <= min_id:
                continue
            if offset_date and m.date <= offset_date:
                continue
            if self.flood_after is not None and i >= self.flood_after:
                self.flood_after = None  # одна вспышка ограничения
                raise FloodWaitError(request=None, capture=self.flood_seconds)
            yield m


def history(n, words=5, start=DATE):
    return [message(i + 1, " ".join(["w"] * words), date=start + timedelta(hours=i)) for i in range(n)]


@pytest.fixture(autouse=True)
def fake_resolve():
    with patch.object(e, "resolve_entity", AsyncMock(return_value=HUMAN)):
        yield


def run_export(client, **kw):
    async def go():
        state = await e.create_job(client, 111, **kw)
        return await e.run_job(client, state["job_id"])

    return asyncio.run(go())


def test_jobs_started_same_second_are_unique():
    fixed = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)

    class Fixed(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed

    with patch.object(e, "datetime", Fixed):
        a = run_export(HistoryClient(history(1)))
        b = run_export(HistoryClient(history(1)))
    assert a["job_id"] != b["job_id"] and a["dir"] != b["dir"]
    assert Path(a["files"][0]).exists() and Path(b["files"][0]).exists()


def test_parts_respect_limits_including_header():
    state = run_export(HistoryClient(history(40, words=20)), max_words=150)
    files = [Path(f) for f in state["files"]]
    assert len(files) > 1
    for f in files:
        assert len(f.read_text(encoding="utf-8").split()) <= 150
    assert all(not p["oversized"] for p in state["parts"])
    manifest = json.loads((Path(state["dir"]) / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "complete" and manifest["messages"] == 40


def test_oversized_message_is_flagged():
    msgs = history(3, words=5)
    msgs[1] = message(2, " ".join(["big"] * 500), date=msgs[1].date)
    state = run_export(HistoryClient(msgs), max_words=100)
    assert e.summary(state)["oversized_parts"] == [state["parts"][1]["name"]]


@pytest.mark.parametrize("bad", [dict(max_words=5), dict(max_mb=0.0001), dict(max_mb=float("inf")), dict(max_mb=0)])
def test_invalid_limits_rejected(bad):
    with pytest.raises(ToolFailure):
        asyncio.run(e.create_job(HistoryClient([]), 111, **bad))


def test_since_is_inclusive_and_until_exclusive():
    msgs = history(5)  # 12:00 … 16:00
    state = run_export(HistoryClient(msgs), since=msgs[1].date.isoformat(), until=msgs[4].date.isoformat())
    text = Path(state["files"][0]).read_text(encoding="utf-8")
    assert [f"#{i}" in text for i in range(1, 6)] == [False, True, True, True, False]


def test_failed_publish_leaves_nothing_public_and_resumes():
    client = HistoryClient(history(10, words=20))
    real_rename = e.os.rename

    async def go():
        state = await e.create_job(client, 111, max_words=150)
        with patch.object(e.os, "rename", side_effect=OSError("disk error")):
            with pytest.raises(OSError):
                await e.run_job(client, state["job_id"])
        failed = e.load_state(state["job_id"])
        assert failed["status"] == "failed"
        assert not (e.EXPORT_DIR / str(failed["peer_id"]) / state["job_id"]).exists()
        with patch.object(e.os, "rename", real_rename):
            return await e.run_job(client, state["job_id"])

    done = asyncio.run(go())
    assert done["status"] == "complete" and done["messages"] == 10


def test_long_flood_pauses_and_resume_continues_without_duplicates():
    client = HistoryClient(history(30, words=20), flood_after=20, flood_seconds=10_000)

    async def go():
        state = await e.create_job(client, 111, max_words=150)
        paused = await e.run_job(client, state["job_id"])
        assert paused["status"] == "paused" and paused["retry_after"] == 10_000
        assert e.summary(paused)["code"] == "flood_wait"
        assert paused["last_id"] > 0
        return await e.run_job(client, state["job_id"])

    done = asyncio.run(go())
    assert done["status"] == "complete" and done["messages"] == 30
    text = "".join(Path(f).read_text(encoding="utf-8") for f in done["files"])
    assert all(text.count(f"· #{i}\n") == 1 for i in range(1, 31))
    assert client.calls[-1]["min_id"] > 0  # продолжение по id, а не с начала


def test_short_flood_is_waited_in_place():
    client = HistoryClient(history(10), flood_after=5, flood_seconds=0)
    state = run_export(client)
    assert state["status"] == "complete" and state["messages"] == 10


def test_manager_runs_in_background_and_reports_interrupted():
    client = HistoryClient(history(3))
    svc = type("Svc", (), {"get": AsyncMock(return_value=client)})()

    async def go():
        mgr = e.ExportManager(svc)
        done = await mgr.start(5, chat=111)
        assert done["status"] == "complete"
        # задание без исполняющего процесса показывается как interrupted
        state = await e.create_job(client, 111)
        state["status"] = "running"
        e._save(state)
        return mgr.status(state["job_id"])

    assert asyncio.run(go())["status"] == "interrupted"


def test_job_id_is_validated():
    with pytest.raises(ToolFailure):
        e.load_state("../../etc")


@pytest.mark.parametrize("failure,code,status", [
    (ToolFailure("session_busy", "Session is busy"), "session_busy", "failed"),
    (RuntimeError("Connection failed"), "export_failed", "failed"),
    (FloodWaitError(request=None, capture=420), "flood_wait", "paused"),
])
def test_resume_reports_connection_failure_and_can_retry(failure, code, status):
    client = HistoryClient(history(3))
    svc = type("Svc", (), {"get": AsyncMock(side_effect=failure)})()

    async def go():
        state = await e.create_job(client, 111)
        state.update(status="paused", retry_after=10_000, error="Previous FloodWait")
        e._save(state)
        mgr = e.ExportManager(svc)
        result = await mgr.resume(state["job_id"], 5)
        assert result["status"] == status and result["code"] == code
        assert result["error"] != "Previous FloodWait"
        saved = e.load_state(state["job_id"])
        assert saved["status"] == status and saved["error"] == result["error"]
        if status == "paused":
            assert result["retry_after"] == 420
        else:
            assert "retry_after" not in result
            assert str(failure) in result["error"]

        svc.get.side_effect = None
        svc.get.return_value = client
        done = await mgr.resume(state["job_id"], 5)
        assert done["status"] == "complete" and done["messages"] == 3
        assert "error" not in done and "code" not in done and "retry_after" not in done

    asyncio.run(go())


def test_status_list_recovers_orphans_and_preserves_running_job(tmp_path, monkeypatch):
    monkeypatch.setattr(e, "JOBS_DIR", tmp_path / ".jobs")
    monkeypatch.setattr(e, "EXPORT_DIR", tmp_path)

    async def go():
        entered, finish = asyncio.Event(), asyncio.Event()

        class BlockingHistory(HistoryClient):
            async def iter_messages(self, *args, **kwargs):
                entered.set()
                await finish.wait()
                async for m in super().iter_messages(*args, **kwargs):
                    yield m

        client = BlockingHistory(history(3))
        svc = type("Svc", (), {"get": AsyncMock(return_value=client)})()
        mgr = e.ExportManager(svc)
        monkeypatch.setattr(server, "exports", mgr)
        orphans = []
        for status in ("created", "running", "waiting_flood"):
            state = await e.create_job(client, 111)
            state.update(status=status, retry_after=120)
            e._save(state)
            orphans.append(state["job_id"])
        live = await mgr.start(0, chat=111)
        try:
            await asyncio.wait_for(entered.wait(), 5)
            result = await server.export_status()
            jobs = {s["job_id"]: s for s in result["jobs"]}
            assert jobs[live["job_id"]]["status"] == "running"
            for job_id in orphans:
                assert jobs[job_id]["status"] == "interrupted"
                assert "retry_after" not in jobs[job_id]
                assert e.load_state(job_id)["status"] == "interrupted"
                assert await server.export_status(job_id) == jobs[job_id]
        finally:
            finish.set()
            await mgr.shutdown()

    asyncio.run(go())


def test_cli_list_preserves_session_owner_and_recovers_after_release(tmp_path, monkeypatch):
    monkeypatch.setattr(e, "JOBS_DIR", tmp_path / ".jobs")
    monkeypatch.setattr(e, "EXPORT_DIR", tmp_path)
    from app.client import SessionLock

    lock_path = tmp_path / "session.lock"
    monkeypatch.setattr(e, "SessionLock", lambda: SessionLock(lock_path))
    state = asyncio.run(e.create_job(HistoryClient([]), 111))
    state.update(status="waiting_flood", retry_after=120)
    e._save(state)
    lock = SessionLock(lock_path)
    lock.acquire()
    try:
        assert e.list_jobs()[0]["status"] == "waiting_flood"
        assert e.load_state(state["job_id"])["status"] == "waiting_flood"
    finally:
        lock.release()
    result = e.list_jobs()[0]
    assert result["status"] == "interrupted" and "retry_after" not in result
    assert e.load_state(state["job_id"])["status"] == "interrupted"
