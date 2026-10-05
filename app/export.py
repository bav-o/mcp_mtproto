"""Выгрузка истории чата в Markdown — возобновляемое задание с разбивкой на части.

Задание (job) живёт в data/exports/.jobs/<job_id>/: state.json (параметры, статус, id последнего
сообщения, попавшего в готовую часть) и parts/ с готовыми частями. После последней части в
parts/ пишется manifest.json со статусом complete, и вся папка одним rename публикуется как
data/exports/<chat_id>/<job_id>/ — полуготовый набор частей наружу не попадает.

Части: <NNN>_<первая дата>_<последняя дата>.md. Новая часть начинается, когда следующее
сообщение вместе с заголовком части не влезает в лимит слов или размера (по умолчанию
400 тыс. слов и 10 МБ — для NotebookLM и Perplexity), а с by_month — ещё и с каждого месяца.
Сообщение, которое само больше лимита, не режется: оно идёт отдельной частью, помеченной
oversized в state.json/manifest.json и в результате.

Интервал дат [since, until) — как у get_history. FloodWait до MAX_AUTO_FLOOD_WAIT_S пережидается
с продолжения с последнего сообщения; более долгий ставит задание на паузу (status=paused,
retry_after). Прерванное/приостановленное задание продолжается с последней готовой части.

MCP-сервер запускает задания в фоне (export_chat_md / export_status / export_resume /
export_cancel). Из терминала (когда MCP-сервер не запущен — сессией владеет один процесс):
    python -m app.export <chat> [--since 2026-01-01] [--until 2026-10-01]
                                [--max-words 400000] [--max-mb 10] [--by-month]
    python -m app.export --resume <job_id>
    python -m app.export --list
chat — id, @username или ссылка t.me/...
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import math
import os
import re
import uuid
from collections.abc import Callable
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import Channel, Chat, MessageService, User
from telethon.utils import get_peer_id

from app.client import DATA_DIR, TelegramService, resolve_entity
from app.common import ToolFailure, parse_range, read_json, write_json_atomic

log = logging.getLogger("tg-collector")

EXPORT_DIR = DATA_DIR / "exports"
JOBS_DIR = EXPORT_DIR / ".jobs"

DEFAULT_MAX_WORDS = 400_000  # NotebookLM: до 500 тыс. слов на источник — с запасом
DEFAULT_MAX_MB = 10  # Perplexity: около 25 МБ на файл, но поиск по большим файлам хуже
MAX_MB_LIMIT = 200  # NotebookLM не примет источник больше 200 МБ
MIN_MAX_WORDS = 100
MIN_MAX_BYTES = 4096
MAX_AUTO_FLOOD_WAIT_S = 15 * 60

_JOB_ID_RE = re.compile(r"^\d{8}-\d{6}-[0-9a-f]{8}$")
_ACTIVE = {"created", "running", "waiting_flood"}
_cancel_requested: set[str] = set()


def _display_name(e: Any) -> str:
    if e is None:
        return "неизвестно"
    if isinstance(e, User):
        name = " ".join(filter(None, [e.first_name, e.last_name])) or "без имени"
    elif isinstance(e, (Channel, Chat)):
        name = e.title
    else:
        name = str(getattr(e, "id", "?"))
    username = getattr(e, "username", None)
    return f"{name} (@{username})" if username else name


def _media_line(m: Any) -> str | None:
    if not m.media:
        return None
    f = m.file
    parts = [type(m.media).__name__.removeprefix("MessageMedia")]
    if f is not None:
        if f.name:
            parts.append(f.name)
        if f.mime_type:
            parts.append(f.mime_type)
        if f.size:
            parts.append(f"{f.size / 1024 / 1024:.1f} МБ")
    return f"📎 {' · '.join(parts)} — скачать: download_media(message_ids=[{m.id}])"


def _format_message(m: Any) -> str:
    header = f"### {m.date.astimezone(timezone.utc):%Y-%m-%d %H:%M} UTC · {_display_name(m.sender)} · #{m.id}"
    lines = [header, ""]
    if isinstance(m, MessageService):
        lines.append(f"_служебное: {type(m.action).__name__.removeprefix('MessageAction')}_")
    else:
        if m.fwd_from:
            fwd = m.fwd_from
            src = fwd.from_name or (get_peer_id(fwd.from_id) if fwd.from_id else None) or "скрыто"
            lines.append(f"> переслано от: {src}")
        if m.reply_to_msg_id:
            lines.append(f"> ответ на #{m.reply_to_msg_id}")
        if m.text:
            lines.append(m.text)  # m.text — уже в markdown (parse_mode клиента по умолчанию)
        media = _media_line(m)
        if media:
            lines.append(media)
    return "\n".join(lines) + "\n\n"


def _part_header(title: str, n: int, first: str, last: str) -> str:
    return f"# {title} — часть {n} ({first} … {last})\n\n"


class _Chunker:
    """Копит сообщения и пишет часть, когда следующее (вместе с заголовком части) не влезет в лимиты."""

    def __init__(
        self,
        parts_dir: Path,
        title: str,
        max_words: int,
        max_bytes: int,
        on_flush: Callable[[dict, int], None],
        start_index: int = 0,
    ) -> None:
        self.parts_dir, self.title = parts_dir, title
        self.max_words, self.max_bytes = max_words, max_bytes
        self.on_flush = on_flush
        self.index = start_index  # сколько частей уже записано
        self._reset()

    def _reset(self) -> None:
        self.buf: list[str] = []
        self.words = self.bytes = 0
        self.first: datetime | None = None
        self.last: datetime | None = None
        self.last_id = 0

    def _header_cost(self) -> tuple[int, int]:
        # даты в заголовке всегда YYYY-MM-DD — считаем по заглушке той же длины
        h = _part_header(self.title, self.index + 1, "0000-00-00", "0000-00-00")
        return len(h.split()), len(h.encode("utf-8"))

    def add(self, text: str, msg_id: int, date: datetime, new_file: bool = False) -> None:
        words, size = len(text.split()), len(text.encode("utf-8"))
        hw, hb = self._header_cost()
        too_big = hw + self.words + words > self.max_words or hb + self.bytes + size > self.max_bytes
        if self.buf and (new_file or too_big):
            self.flush()
        self.buf.append(text)
        self.words += words
        self.bytes += size
        self.first = self.first or date
        self.last = date
        self.last_id = msg_id

    def flush(self) -> None:
        if not self.buf:
            return
        n = self.index + 1
        first, last = f"{self.first:%Y-%m-%d}", f"{self.last:%Y-%m-%d}"
        content = _part_header(self.title, n, first, last) + "".join(self.buf)
        name = f"{n:03d}_{first}_{last}.md"
        tmp = self.parts_dir / f".{name}.tmp"
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, self.parts_dir / name)
        words, size = len(content.split()), len(content.encode("utf-8"))
        info = {
            "name": name,
            "messages": len(self.buf),
            "words": words,
            "bytes": size,
            # одно сообщение больше лимита: не режем, а помечаем
            "oversized": words > self.max_words or size > self.max_bytes,
        }
        self.index = n
        last_id = self.last_id
        self._reset()
        self.on_flush(info, last_id)


# ---------- состояние задания ----------


def _job_dir(job_id: str) -> Path:
    return JOBS_DIR / job_id


def _save(state: dict) -> None:
    state["updated"] = datetime.now(timezone.utc).isoformat()
    write_json_atomic(_job_dir(state["job_id"]) / "state.json", state)


def load_state(job_id: str) -> dict:
    state = read_json(_job_dir(job_id) / "state.json") if _JOB_ID_RE.match(job_id or "") else None
    if not isinstance(state, dict):
        raise ToolFailure("not_found", f"задание выгрузки {job_id!r} не найдено")
    return state


def list_jobs(limit: int = 20) -> list[dict]:
    if not JOBS_DIR.is_dir():
        return []
    ids = sorted((p.name for p in JOBS_DIR.iterdir() if _JOB_ID_RE.match(p.name)), reverse=True)
    out = []
    for job_id in ids[:limit]:
        with suppress(ToolFailure):
            out.append(summary(load_state(job_id)))
    return out


def summary(state: dict) -> dict:
    """То, что видит пользователь: без внутренних полей задания."""
    status = state["status"]
    out: dict[str, Any] = {
        "ok": status not in ("failed", "paused"),
        "job_id": state["job_id"],
        "status": status,
        "chat": state["title"],
        "messages": state["messages"],
        "parts": len(state["parts"]),
    }
    if status == "complete":
        out["dir"] = state["dir"]
        out["files"] = state["files"]
        out["oversized_parts"] = [p["name"] for p in state["parts"] if p["oversized"]]
    if status == "paused":
        out["code"] = "flood_wait"
    if status == "failed":
        out["code"] = "export_failed"
    if state.get("retry_after"):
        out["retry_after"] = state["retry_after"]
    if state.get("error"):
        out["error"] = state["error"]
    if status in ("paused", "interrupted", "failed", "cancelled"):
        out["resume"] = f"export_resume(job_id={state['job_id']!r}) или python -m app.export --resume {state['job_id']}"
    if status in _ACTIVE:
        out["note"] = "выгрузка идёт в фоне — проверяйте export_status(job_id)"
    return out


def _validate_limits(max_words: int, max_mb: float) -> int:
    if not isinstance(max_words, int) or max_words < MIN_MAX_WORDS:
        raise ToolFailure("invalid_argument", f"max_words должен быть целым не меньше {MIN_MAX_WORDS}")
    if not isinstance(max_mb, (int, float)) or not math.isfinite(max_mb) or not 0 < max_mb <= MAX_MB_LIMIT:
        raise ToolFailure("invalid_argument", f"max_mb должен быть конечным числом от 0 до {MAX_MB_LIMIT}")
    max_bytes = int(max_mb * 1024 * 1024)
    if max_bytes < MIN_MAX_BYTES:
        raise ToolFailure("invalid_argument", f"max_mb слишком мал: часть должна вмещать хотя бы {MIN_MAX_BYTES} байт")
    return max_bytes


async def create_job(
    client: TelegramClient,
    chat: str | int,
    since: str | None = None,
    until: str | None = None,
    max_words: int = DEFAULT_MAX_WORDS,
    max_mb: float = DEFAULT_MAX_MB,
    by_month: bool = False,
) -> dict:
    max_bytes = _validate_limits(max_words, max_mb)
    parse_range(since, until)
    entity = await resolve_entity(client, chat)
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    while True:  # уникальный id даже для запусков в одну и ту же секунду
        job_id = f"{datetime.now(timezone.utc):%Y%m%d-%H%M%S}-{uuid.uuid4().hex[:8]}"
        try:
            _job_dir(job_id).mkdir()
            break
        except FileExistsError:
            continue
    (_job_dir(job_id) / "parts").mkdir()
    state = {
        "job_id": job_id,
        "chat": chat,
        "peer_id": get_peer_id(entity),
        "title": _display_name(entity),
        "since": since,
        "until": until,
        "max_words": max_words,
        "max_bytes": max_bytes,
        "by_month": by_month,
        "status": "created",
        "last_id": 0,
        "messages": 0,
        "parts": [],
        "created": datetime.now(timezone.utc).isoformat(),
    }
    _save(state)
    return state


async def _collect(
    client: TelegramClient,
    entity: Any,
    state: dict,
    chunker: _Chunker,
    max_auto_flood_wait: int,
) -> None:
    since_dt, until_dt = parse_range(state["since"], state["until"])
    last_seen = state["last_id"]
    month = None
    while True:
        kwargs: dict[str, Any] = {"reverse": True}
        if last_seen:
            kwargs["min_id"] = last_seen  # при reverse=True — «только сообщения новее этого id»
        elif since_dt:
            # offset_date исключающий: берём на секунду раньше, точную границу режем сами
            kwargs["offset_date"] = since_dt - timedelta(seconds=1)
        try:
            async for m in client.iter_messages(entity, **kwargs):
                last_seen = m.id
                if since_dt and m.date < since_dt:
                    continue
                if until_dt and m.date >= until_dt:
                    return
                date = m.date.astimezone(timezone.utc)
                m_month = f"{date:%Y-%m}"
                new_file = state["by_month"] and month is not None and m_month != month
                chunker.add(_format_message(m), m.id, date, new_file=new_file)
                month = m_month
            return
        except FloodWaitError as e:
            if e.seconds > max_auto_flood_wait:
                raise
            log.warning("export %s: FloodWait %ds — жду и продолжаю с #%s", state["job_id"], e.seconds, last_seen)
            state.update(status="waiting_flood", retry_after=e.seconds)
            _save(state)
            await asyncio.sleep(e.seconds)
            state.update(status="running", retry_after=None)
            _save(state)


def _publish(state: dict) -> None:
    parts_dir = _job_dir(state["job_id"]) / "parts"
    final_dir = EXPORT_DIR / str(state["peer_id"]) / state["job_id"]
    completed = datetime.now(timezone.utc).isoformat()
    write_json_atomic(
        parts_dir / "manifest.json",
        {
            "status": "complete",
            "job_id": state["job_id"],
            "chat": state["title"],
            "peer_id": state["peer_id"],
            "since": state["since"],
            "until": state["until"],
            "messages": state["messages"],
            "parts": state["parts"],
            "completed": completed,
        },
    )
    final_dir.parent.mkdir(parents=True, exist_ok=True)
    os.rename(parts_dir, final_dir)  # одна атомарная операция для всего набора
    _mark_complete(state, final_dir)


def _mark_complete(state: dict, final_dir: Path) -> None:
    state.update(
        status="complete",
        dir=str(final_dir),
        files=[str(final_dir / p["name"]) for p in state["parts"]],
        error=None,
        retry_after=None,
    )
    _save(state)


async def run_job(client: TelegramClient, job_id: str, *, max_auto_flood_wait: int = MAX_AUTO_FLOOD_WAIT_S) -> dict:
    """Выполнить или продолжить задание. Возвращает итоговое состояние (complete/paused);
    при ошибке или отмене состояние сохраняется (failed/interrupted/cancelled) и исключение
    пробрасывается."""
    state = load_state(job_id)
    if state["status"] == "complete":
        return state
    parts_dir = _job_dir(job_id) / "parts"
    final_dir = EXPORT_DIR / str(state["peer_id"]) / job_id
    if not parts_dir.is_dir():
        if (final_dir / "manifest.json").is_file():  # rename прошёл, но состояние не успели записать
            _mark_complete(state, final_dir)
            return state
        raise ToolFailure("export_failed", f"папка частей задания {job_id} пропала — начните выгрузку заново")
    try:
        known = {p["name"] for p in state["parts"]}
        for f in parts_dir.iterdir():  # части, записанные после последнего сохранения состояния
            if f.name not in known:
                f.unlink()
        state.update(status="running", error=None, retry_after=None)
        _save(state)
        entity = await resolve_entity(client, state["peer_id"])

        def on_flush(info: dict, last_id: int) -> None:
            state["parts"].append(info)
            state["last_id"] = last_id
            state["messages"] += info["messages"]
            _save(state)

        chunker = _Chunker(
            parts_dir, state["title"], state["max_words"], state["max_bytes"], on_flush, start_index=len(state["parts"])
        )
        await _collect(client, entity, state, chunker, max_auto_flood_wait)
        chunker.flush()
        _publish(state)
    except FloodWaitError as e:
        state.update(
            status="paused",
            retry_after=e.seconds,
            error=f"Telegram просит подождать {e.seconds} с — продолжите выгрузку позже",
        )
        _save(state)
    except asyncio.CancelledError:
        state["status"] = "cancelled" if job_id in _cancel_requested else "interrupted"
        _cancel_requested.discard(job_id)
        _save(state)
        raise
    except Exception as e:
        state.update(status="failed", error=e.message if isinstance(e, ToolFailure) else f"{type(e).__name__}: {e}")
        _save(state)
        raise
    log.info("export %s: %s, messages=%d parts=%d", job_id, state["status"], state["messages"], len(state["parts"]))
    return state


class ExportManager:
    """Фоновые задания выгрузки внутри MCP-сервера: старт, статус, продолжение, отмена."""

    def __init__(self, service: TelegramService) -> None:
        self._service = service
        self._tasks: dict[str, asyncio.Task] = {}

    async def start(self, wait_seconds: float, **params: Any) -> dict:
        client = await self._service.get()
        state = await create_job(client, **params)
        return await self._launch(state["job_id"], wait_seconds)

    async def resume(self, job_id: str, wait_seconds: float) -> dict:
        state = load_state(job_id)
        if job_id in self._tasks or state["status"] == "complete":
            return self.status(job_id)
        return await self._launch(job_id, wait_seconds)

    async def _launch(self, job_id: str, wait_seconds: float) -> dict:
        task = asyncio.create_task(self._run(job_id), name=f"export-{job_id}")
        self._tasks[job_id] = task
        task.add_done_callback(lambda _t: self._tasks.pop(job_id, None))
        await asyncio.wait({task}, timeout=wait_seconds)  # не отменяет задание по таймауту
        return self.status(job_id)

    async def _run(self, job_id: str) -> None:
        try:
            await run_job(await self._service.get(), job_id)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — статус и текст ошибки уже в state.json
            log.exception("export %s failed", job_id)

    def status(self, job_id: str) -> dict:
        state = load_state(job_id)
        if state["status"] in _ACTIVE and job_id not in self._tasks:
            state["status"] = "interrupted"  # процесс, который его выполнял, завершился
            _save(state)
        return summary(state)

    async def cancel(self, job_id: str) -> dict:
        state = load_state(job_id)
        task = self._tasks.get(job_id)
        if task is not None:
            _cancel_requested.add(job_id)
            task.cancel()
            await asyncio.wait({task})
        elif state["status"] != "complete":
            state["status"] = "cancelled"
            _save(state)
        return self.status(job_id)

    async def shutdown(self) -> None:
        tasks = list(self._tasks.values())
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


# ---------- терминал ----------


def _print_summary(s: dict) -> None:
    print(f"Задание {s['job_id']}: {s['status']}, сообщений: {s['messages']}, частей: {s['parts']}")
    if s.get("dir"):
        print(f"  → {s['dir']}")
        for f in s["files"]:
            print(f"  {f}")
        if s["oversized_parts"]:
            print(f"  больше лимита (одно крупное сообщение): {', '.join(s['oversized_parts'])}")
    if s.get("error"):
        print(f"  {s['error']}")
    if s.get("resume"):
        print(f"  продолжить: python -m app.export --resume {s['job_id']}")


async def _main() -> None:
    p = argparse.ArgumentParser(description="Выгрузить историю чата Telegram в Markdown")
    p.add_argument("chat", nargs="?", help="id, @username или ссылка t.me/...")
    p.add_argument("--since", help="ISO-дата начала (включительно), например 2026-01-01")
    p.add_argument("--until", help="ISO-дата конца (не включительно)")
    p.add_argument("--max-words", type=int, default=DEFAULT_MAX_WORDS, help=f"слов на часть (по умолчанию {DEFAULT_MAX_WORDS})")
    p.add_argument("--max-mb", type=float, default=DEFAULT_MAX_MB, help=f"МБ на часть (по умолчанию {DEFAULT_MAX_MB})")
    p.add_argument("--by-month", action="store_true", help="дополнительно начинать новую часть с каждого месяца")
    p.add_argument("--resume", metavar="JOB_ID", help="продолжить прерванную/приостановленную выгрузку")
    p.add_argument("--list", action="store_true", help="показать последние задания выгрузки")
    args = p.parse_args()

    if args.list:
        for s in list_jobs():
            _print_summary(s)
        return
    if not args.chat and not args.resume:
        p.error("укажите чат или --resume JOB_ID")
    try:
        async with TelegramService() as client:
            if args.resume:
                job_id = args.resume
            else:
                params = dict(since=args.since, until=args.until, max_words=args.max_words, max_mb=args.max_mb)
                job_id = (await create_job(client, args.chat, by_month=args.by_month, **params))["job_id"]
                print(f"Задание {job_id} создано")
            try:
                # в терминале таймаута MCP нет — пережидаем FloodWait до суток
                await run_job(client, job_id, max_auto_flood_wait=24 * 3600)
            finally:
                _print_summary(summary(load_state(job_id)))
    except ToolFailure as e:
        raise SystemExit(e.message) from None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        raise SystemExit("Прервано — продолжить: python -m app.export --list, затем --resume <job_id>") from None
