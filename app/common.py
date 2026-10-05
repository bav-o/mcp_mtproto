"""Общие типы и утилиты: единый формат ошибок инструментов, даты, атомарная запись, блокировки по ключу."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from collections.abc import AsyncIterator, Hashable
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class ToolFailure(Exception):
    """Ожидаемая ошибка инструмента: превращается в {"ok": false, "code": ..., "error": ...}."""

    def __init__(self, code: str, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.code, self.message, self.extra = code, message, extra

    def as_dict(self) -> dict:
        return {"ok": False, "code": self.code, "error": self.message, **self.extra}


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        raise ToolFailure("invalid_argument", f"не ISO-дата: {s!r} (пример: 2026-09-01 или 2026-09-01T12:00:00)") from None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_range(since: str | None, until: str | None) -> tuple[datetime | None, datetime | None]:
    """Интервал [since, until): since включительно, until — нет. Одинаков для истории и экспорта."""
    since_dt, until_dt = parse_dt(since), parse_dt(until)
    if since_dt and until_dt and since_dt >= until_dt:
        raise ToolFailure("invalid_argument", "since должен быть раньше until")
    return since_dt, until_dt


def check_range(name: str, value: int, low: int, high: int) -> None:
    if not low <= value <= high:
        raise ToolFailure("invalid_argument", f"{name} должен быть от {low} до {high}")


def write_json_atomic(path: Path, data: Any) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


class KeyedLocks:
    """asyncio.Lock на каждый ключ; блокировки удаляются, когда их никто не держит и не ждёт."""

    def __init__(self) -> None:
        self._locks: dict[Hashable, asyncio.Lock] = {}
        self._users: dict[Hashable, int] = {}

    @asynccontextmanager
    async def hold(self, key: Hashable) -> AsyncIterator[None]:
        lock = self._locks.setdefault(key, asyncio.Lock())
        self._users[key] = self._users.get(key, 0) + 1
        try:
            async with lock:
                yield
        finally:
            self._users[key] -= 1
            if not self._users[key]:
                del self._users[key], self._locks[key]
