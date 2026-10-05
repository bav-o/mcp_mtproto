"""Загрузка медиа с проверяемым кэшем.

data/media/<chat_id>/<msg_id><ext> — файл, <msg_id>.meta.json — что в нём лежит: id вложения
в Telegram (photo/document id), ожидаемый и фактический размер. Кэш используется, только если
метаданные совпадают с текущим вложением сообщения — заменённое в Telegram вложение
скачивается заново.

Каждая загрузка пишет в свой временный файл (.<msg_id>.<random>.part), проверяет размер и
только потом атомарно публикуется. Одинаковые запросы (чат, сообщение) сериализуются.
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import Any

from telethon import TelegramClient
from telethon.tl.types import Document

from app.client import DATA_DIR
from app.common import KeyedLocks, ToolFailure, read_json, write_json_atomic
from app.flood import retry_on_flood

log = logging.getLogger("tg-collector")

MEDIA_DIR = DATA_DIR / "media"
_locks = KeyedLocks()


def media_key(m: Any) -> str | None:
    """Идентичность вложения: меняется, если в сообщении заменили файл/фото."""
    obj = getattr(m.media, "photo", None) or getattr(m.media, "document", None)
    obj_id = getattr(obj, "id", None)
    return f"{type(obj).__name__}:{obj_id}" if obj_id is not None else None


def _meta_path(target_dir: Path, mid: int) -> Path:
    return target_dir / f"{mid}.meta.json"


def cached_file(target_dir: Path, mid: int, key: str | None, size: int) -> Path | None:
    meta = read_json(_meta_path(target_dir, mid))
    if not isinstance(meta, dict) or key is None:
        return None
    if meta.get("media_key") != key or meta.get("size") != size:
        return None
    path = target_dir / str(meta.get("file", ""))
    try:
        if path.is_file() and path.stat().st_size == meta.get("bytes"):
            return path
    except OSError:
        pass
    return None


async def download_message_media(client: TelegramClient, m: Any, target_dir: Path, size: int) -> tuple[Path, bool]:
    """Скачать вложение сообщения m в target_dir. Возвращает (путь, взят_из_кэша)."""
    key = media_key(m)
    async with _locks.hold((str(target_dir), m.id)):
        cached = cached_file(target_dir, m.id, key, size)
        if cached:
            return cached, True
        # остатки оборванных загрузок этого сообщения: параллельных нет — мы под блокировкой,
        # а другой процесс сессию не держит (SessionLock)
        for stale in target_dir.glob(f".{m.id}.*.part"):
            stale.unlink(missing_ok=True)

        fd, tmp = tempfile.mkstemp(dir=target_dir, prefix=f".{m.id}.", suffix=".part")
        os.close(fd)
        try:
            # файл уже существует — Telethon пишет ровно в него, не меняя имя
            path = await retry_on_flood(lambda: client.download_media(m, file=tmp))
            if not path:
                raise ToolFailure("unsupported_media", "Telegram не отдал файл (тип медиа не поддерживается)")
            if os.path.abspath(path) != os.path.abspath(tmp):
                os.replace(path, tmp)
            actual = os.path.getsize(tmp)
            # для документов Telegram сообщает точный размер; для фото size — оценка самого
            # крупного превью, поэтому там проверяем только, что файл не пустой
            exact = isinstance(getattr(m.media, "document", None), Document)
            if actual == 0 or (exact and actual != size):
                raise ToolFailure(
                    "incomplete_download",
                    f"файл скачан не полностью: {actual} из {size} байт — повторите запрос",
                )
            final = target_dir / f"{m.id}{m.file.ext or '.bin'}"
            old = read_json(_meta_path(target_dir, m.id))
            os.replace(tmp, final)
            write_json_atomic(
                _meta_path(target_dir, m.id),
                {"file": final.name, "media_key": key, "size": size, "bytes": actual},
            )
            if isinstance(old, dict) and old.get("file") and old["file"] != final.name:
                (target_dir / old["file"]).unlink(missing_ok=True)  # прежнее вложение с другим расширением
        except BaseException:  # включая CancelledError при таймауте клиента
            Path(tmp).unlink(missing_ok=True)
            raise
        log.info("download_media: msg=%s -> %s", m.id, final)
        return final, False
