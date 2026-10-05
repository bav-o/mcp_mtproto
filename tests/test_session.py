"""Владение сессией, cleanup при сбое получения клиента, права файлов (ревью, пп. 2, 7, 9)."""

import asyncio
import os
import runpy
import stat
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import app.client as c
from app.common import ToolFailure


@pytest.mark.parametrize("configured,override", [(False, False), (True, False), (True, True)])
def test_data_directory_configuration(tmp_path, monkeypatch, configured, override):
    # Исполняем копию модуля с отдельным ROOT и настоящим dotenv, не читая .env проекта.
    source = tmp_path / "app" / "client.py"
    source.parent.mkdir()
    source.write_text(Path(c.__file__).read_text(encoding="utf-8"), encoding="utf-8")
    dotenv_dir = tmp_path / "dotenv-data"
    external_dir = tmp_path / "external-data"
    if configured:
        (tmp_path / ".env").write_text(f'MTPROTO_DATA_DIR={dotenv_dir.as_posix()}\n', encoding="utf-8")
    monkeypatch.delenv("PYTHON_DOTENV_DISABLED", raising=False)
    monkeypatch.delenv("MTPROTO_DATA_DIR", raising=False)
    if override:
        monkeypatch.setenv("MTPROTO_DATA_DIR", str(external_dir))

    config = runpy.run_path(str(source))

    expected = external_dir if override else dotenv_dir if configured else tmp_path / "data"
    assert config["DATA_DIR"] == expected
    assert config["SESSION_FILE"] == expected / "session.session"
    assert config["LOCK_FILE"] == expected / "session.lock"


def test_session_lock_is_exclusive(tmp_path):
    a, b = c.SessionLock(tmp_path / "s.lock"), c.SessionLock(tmp_path / "s.lock")
    a.acquire()
    with pytest.raises(ToolFailure) as err:
        b.acquire()
    assert err.value.code == "session_busy"
    a.release()
    b.acquire()  # после освобождения — доступна
    b.release()


def test_service_reuses_one_client_and_cleans_up_on_failure(data_dir):
    c.SESSION_FILE.touch()
    good = MagicMock(connect=AsyncMock(), is_user_authorized=AsyncMock(return_value=True), disconnect=AsyncMock())
    good.is_connected.return_value = True
    bad = MagicMock(connect=AsyncMock(), disconnect=AsyncMock(),
                    is_user_authorized=AsyncMock(side_effect=RuntimeError("synthetic failure")))

    async def run():
        svc = c.TelegramService()
        with patch.object(c, "TelegramClient", side_effect=[bad, good]) as factory:
            with pytest.raises(RuntimeError):
                await svc.get()
            bad.disconnect.assert_awaited_once()
            assert svc._session_lock._fh is None  # блокировка снята
            first, second = await asyncio.gather(svc.get(), svc.get())
            assert first is second is good and factory.call_count == 2
        # пока сервис держит сессию, второй владелец не получит её
        with pytest.raises(ToolFailure):
            c.SessionLock().acquire()
        await svc.close()
        good.disconnect.assert_awaited_once()
        c.SessionLock().acquire()

    asyncio.run(run())


@pytest.mark.skipif(os.name == "nt", reason="POSIX-права")
def test_storage_is_private_before_session_written(data_dir):
    os.chmod(data_dir, 0o755)
    c.SESSION_FILE.unlink(missing_ok=True)
    old = os.umask(0o022)
    try:
        c.prepare_storage(create_session=True)
    finally:
        os.umask(old)
    assert stat.S_IMODE(data_dir.stat().st_mode) == 0o700
    assert stat.S_IMODE(c.SESSION_FILE.stat().st_mode) == 0o600
    os.chmod(c.SESSION_FILE, 0o644)
    c.prepare_storage()
    assert stat.S_IMODE(c.SESSION_FILE.stat().st_mode) == 0o600
