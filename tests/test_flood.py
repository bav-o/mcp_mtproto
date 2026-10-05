"""FloodWait: короткий — пережидаем, длинный — сразу retry_after (ревью, п. 8)."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from telethon.errors import FloodWaitError

import app.server as server
from app.flood import retry_on_flood


def test_long_wait_is_not_slept_inside_request():
    func = AsyncMock(side_effect=FloodWaitError(request=None, capture=3600))
    with pytest.raises(FloodWaitError):
        asyncio.run(retry_on_flood(func, max_wait=60))
    assert func.await_count == 1


def test_short_wait_is_retried():
    func = AsyncMock(side_effect=[FloodWaitError(request=None, capture=0), "ok"])
    assert asyncio.run(retry_on_flood(func)) == "ok"


def test_tool_reports_structured_retry_after():
    with patch.object(server.telegram, "get", AsyncMock(side_effect=FloodWaitError(request=None, capture=420))):
        result = asyncio.run(server.list_dialogs())
    assert result == {
        "ok": False,
        "code": "flood_wait",
        "error": "Telegram ограничил частоту запросов — повторите через 420 с",
        "retry_after": 420,
    }
