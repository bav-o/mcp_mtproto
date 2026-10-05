"""MCP-сервер сбора данных из Telegram: группы/каналы (история, поиск, участники)
и работа с ботами (отправить команду, нажать кнопку, прочитать ответ).

Один Telegram-аккаунт пользователя (Telethon, data/session.session), работает только
локально на компьютере пользователя. Присоединение к новым публичным каналам — явное действие
инструмента join_channel, каждый раз логируется.

Запуск (из .mcp.json хост-процессом Claude Code):
    python -m app.server
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from mcp.server.fastmcp import FastMCP
from telethon.errors import (
    ChannelPrivateError,
    ChannelsTooMuchError,
    ChatAdminRequiredError,
    FloodWaitError,
    UserAlreadyParticipantError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import Channel, Chat, User
from telethon.utils import get_peer_id

from app.client import DATA_DIR, make_client, session_exists
from app.flood import retry_on_flood

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tg-collector")

mcp = FastMCP("tg-collector")

_BOT_WAIT_TIMEOUT_S = 20
_MAX_MEDIA_PER_CALL = 50
_MAX_MEDIA_SIZE_MB = 200
MEDIA_DIR = DATA_DIR / "media"


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def _entity_kind(e: Any) -> str:
    if isinstance(e, User):
        return "bot" if e.bot else "user"
    if isinstance(e, Channel):
        return "channel" if e.broadcast else "supergroup"
    if isinstance(e, Chat):
        return "group"
    return "unknown"


async def _get_client():
    if not session_exists():
        raise RuntimeError(
            "Сессия не найдена. Сначала выполните вход: "
            ".venv/bin/python -m app.auth (один раз, руками, спросит код из Telegram)."
        )
    client = make_client()
    await client.connect()
    if not await retry_on_flood(client.is_user_authorized):
        await client.disconnect()
        raise RuntimeError("Сессия протухла или отозвана — повторите вход через app.auth")
    return client


def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@mcp.tool()
async def list_dialogs(limit: int = 50) -> list[dict]:
    """Список чатов аккаунта: личные, группы, каналы, боты — с id и названием.

    Используйте, чтобы узнать точный chat_id/username перед вызовом остальных инструментов.
    """
    client = await _get_client()
    try:
        out = []
        async for d in client.iter_dialogs(limit=limit):
            out.append(
                {
                    "id": d.id,
                    "title": d.name,
                    "username": getattr(d.entity, "username", None),
                    "kind": _entity_kind(d.entity),
                    "unread_count": d.unread_count,
                }
            )
        return out
    finally:
        await client.disconnect()


@mcp.tool()
async def join_channel(link_or_username: str) -> dict:
    """Присоединиться к публичному каналу/группе по @username или ссылке-приглашению (t.me/+...).

    Действие видимое и необратимое без выхода из канала — каждый вызов логируется.
    После этого канал станет доступен в list_dialogs/get_history/search_messages.
    """
    client = await _get_client()
    try:
        link = link_or_username.strip()
        try:
            if "joinchat" in link or "/+" in link:
                invite_hash = link.rstrip("/").split("/")[-1].lstrip("+")
                result = await retry_on_flood(lambda: client(ImportChatInviteRequest(invite_hash)))
                chat = result.chats[0]
            else:
                username = link.split("t.me/")[-1].lstrip("@")
                entity = await client.get_entity(username)
                await retry_on_flood(lambda: client(JoinChannelRequest(entity)))
                chat = entity
            log.info("join_channel: joined %r (id=%s)", getattr(chat, "title", link), chat.id)
            return {"ok": True, "id": chat.id, "title": getattr(chat, "title", None)}
        except UserAlreadyParticipantError:
            entity = await client.get_entity(link.split("t.me/")[-1].lstrip("@"))
            return {"ok": True, "already_member": True, "id": entity.id, "title": getattr(entity, "title", None)}
        except (UsernameInvalidError, UsernameNotOccupiedError):
            return {"ok": False, "error": "канал/username не найден"}
        except ChannelsTooMuchError:
            return {"ok": False, "error": "превышен лимит каналов на аккаунт (слишком много подписок)"}
        except ChannelPrivateError:
            return {"ok": False, "error": "канал приватный — нужна действующая пригласительная ссылка"}
    finally:
        await client.disconnect()


@mcp.tool()
async def get_history(
    chat: str,
    since: str | None = None,
    until: str | None = None,
    limit: int = 200,
) -> list[dict]:
    """История сообщений чата за период.

    chat — id, @username или ссылка t.me/...
    since/until — ISO-даты ("2026-09-01" или "2026-09-01T00:00:00"), можно опустить любую.
    limit — не больше стольки сообщений (защита от случайной выгрузки всей истории разом).
    """
    client = await _get_client()
    try:
        entity = await retry_on_flood(lambda: client.get_entity(chat))
        since_dt, until_dt = _parse_dt(since), _parse_dt(until)
        out = []
        async for m in client.iter_messages(entity, limit=limit, offset_date=until_dt, reverse=False):
            if since_dt and m.date and m.date < since_dt:
                break
            out.append(
                {
                    "id": m.id,
                    "date": _iso(m.date),
                    "sender_id": m.sender_id,
                    "sender": (getattr(m.sender, "username", None) or getattr(m.sender, "first_name", None)) if m.sender else None,
                    "text": m.message or None,
                    "has_media": m.media is not None,
                    "media_type": type(m.media).__name__ if m.media else None,
                }
            )
        out.reverse()
        return out
    finally:
        await client.disconnect()


@mcp.tool()
async def search_messages(chat: str, query: str, limit: int = 100) -> list[dict]:
    """Поиск сообщений по ключевым словам внутри одного чата (серверный поиск Telegram)."""
    client = await _get_client()
    try:
        entity = await retry_on_flood(lambda: client.get_entity(chat))
        out = []
        async for m in client.iter_messages(entity, search=query, limit=limit):
            out.append(
                {
                    "id": m.id,
                    "date": _iso(m.date),
                    "sender_id": m.sender_id,
                    "text": m.message or None,
                    "has_media": m.media is not None,
                    "media_type": type(m.media).__name__ if m.media else None,
                }
            )
        return out
    finally:
        await client.disconnect()


@mcp.tool()
async def download_media(chat: str, message_ids: list[int], max_size_mb: int = 25) -> list[dict]:
    """Скачать фото/файлы из указанных сообщений чата на диск (data/media/<chat_id>/).

    message_ids берите из get_history/search_messages (поле has_media = true).
    Возвращает локальный путь к файлу — его можно открыть через Read (картинки видны).
    Файлы больше max_size_mb пропускаются, не более 50 сообщений за вызов.
    """
    if not message_ids:
        return []
    if len(message_ids) > _MAX_MEDIA_PER_CALL:
        raise ValueError(f"не больше {_MAX_MEDIA_PER_CALL} сообщений за вызов")
    if not 1 <= max_size_mb <= _MAX_MEDIA_SIZE_MB:
        raise ValueError(f"max_size_mb должен быть от 1 до {_MAX_MEDIA_SIZE_MB}")
    client = await _get_client()
    try:
        entity = await retry_on_flood(lambda: client.get_entity(chat))
        target_dir = MEDIA_DIR / str(get_peer_id(entity))
        target_dir.mkdir(parents=True, exist_ok=True)
        msgs = await retry_on_flood(lambda: client.get_messages(entity, ids=message_ids))
        out: list[dict] = []
        for mid, m in zip(message_ids, msgs):
            try:
                if m is None:
                    out.append({"id": mid, "ok": False, "error": "сообщение не найдено"})
                    continue
                if not m.media:
                    out.append({"id": mid, "ok": False, "error": "в сообщении нет медиа"})
                    continue
                size = getattr(getattr(m, "file", None), "size", None)
                if size is None:
                    out.append({"id": mid, "ok": False, "error": "размер файла неизвестен (превью ссылки или нестандартное медиа)"})
                    continue
                if size > max_size_mb * 1024 * 1024:
                    out.append({"id": mid, "ok": False, "error": f"файл {size // 1024 // 1024} МБ больше лимита {max_size_mb} МБ"})
                    continue
                cached = sorted(p for p in target_dir.glob(f"{mid}.*") if " (" not in p.name)
                if cached:
                    path = str(cached[0])
                else:
                    path = await retry_on_flood(lambda: client.download_media(m, file=str(target_dir / f"{mid}")))
                if not path:
                    out.append({"id": mid, "ok": False, "error": "Telegram не отдал файл (тип медиа не поддерживается)"})
                    continue
                log.info("download_media: chat=%s msg=%s -> %s", entity.id, mid, path)
                out.append({"id": mid, "ok": True, "path": path, "mime": getattr(m.file, "mime_type", None), "size": size})
            except Exception as e:  # noqa: BLE001 — один сбойный файл не должен терять остальные
                log.warning("download_media: msg=%s failed: %r", mid, e)
                out.append({"id": mid, "ok": False, "error": f"{type(e).__name__}: {e}"})
        return out
    finally:
        await client.disconnect()


@mcp.tool()
async def list_participants(chat: str, limit: int = 500) -> list[dict] | dict:
    """Список участников группы/канала (если список открыт — многие каналы его скрывают)."""
    client = await _get_client()
    try:
        entity = await retry_on_flood(lambda: client.get_entity(chat))
        try:
            out = []
            async for u in client.iter_participants(entity, limit=limit):
                out.append(
                    {
                        "id": u.id,
                        "username": u.username,
                        "first_name": u.first_name,
                        "last_name": u.last_name,
                        "is_bot": u.bot,
                    }
                )
            return out
        except ChatAdminRequiredError:
            return {"ok": False, "error": "список участников скрыт — нужны права администратора"}
    finally:
        await client.disconnect()


@mcp.tool()
async def send_to_bot(bot: str, text: str, wait_for_reply: bool = True) -> dict:
    """Отправить сообщение/команду боту (например /start) и дождаться его ответа.

    Возвращает текст ответа и кнопки (если есть), чтобы следующим шагом можно было
    позвать click_button с нужной подписью кнопки.
    """
    client = await _get_client()
    try:
        entity = await retry_on_flood(lambda: client.get_entity(bot))
        reply_box: dict[str, Any] = {}
        done = asyncio.Event()

        if wait_for_reply:
            from telethon import events

            @client.on(events.NewMessage(from_users=entity))
            async def _handler(event):  # noqa: ANN001
                reply_box["message"] = event.message
                done.set()

        await retry_on_flood(lambda: client.send_message(entity, text))
        result: dict[str, Any] = {"sent": text, "bot": bot}

        if wait_for_reply:
            try:
                await asyncio.wait_for(done.wait(), timeout=_BOT_WAIT_TIMEOUT_S)
                m = reply_box["message"]
                result["reply"] = _format_bot_message(m)
            except TimeoutError:
                result["reply"] = None
                result["note"] = f"бот не ответил за {_BOT_WAIT_TIMEOUT_S}с"
        return result
    finally:
        await client.disconnect()


def _format_bot_message(m) -> dict:  # noqa: ANN001
    buttons = []
    if m.buttons:
        for row_i, row in enumerate(m.buttons):
            for col_i, b in enumerate(row):
                buttons.append({"row": row_i, "col": col_i, "text": b.text})
    return {
        "message_id": m.id,
        "date": _iso(m.date),
        "text": m.message or None,
        "buttons": buttons,
    }


@mcp.tool()
async def get_recent_bot_messages(bot: str, limit: int = 10) -> list[dict]:
    """Последние сообщения от бота в этом диалоге — посмотреть состояние перед click_button."""
    client = await _get_client()
    try:
        entity = await retry_on_flood(lambda: client.get_entity(bot))
        out = []
        async for m in client.iter_messages(entity, limit=limit):
            out.append(_format_bot_message(m))
        return out
    finally:
        await client.disconnect()


@mcp.tool()
async def click_button(bot: str, message_id: int, button_text: str, wait_for_reply: bool = True) -> dict:
    """Нажать inline-кнопку на конкретном сообщении бота (по видимому тексту кнопки).

    message_id берите из send_to_bot / get_recent_bot_messages.
    Возвращает результат нажатия: новое/изменённое сообщение бота, если дождались.
    """
    client = await _get_client()
    try:
        entity = await retry_on_flood(lambda: client.get_entity(bot))
        msgs = await retry_on_flood(lambda: client.get_messages(entity, ids=message_id))
        if not msgs:
            return {"ok": False, "error": f"сообщение {message_id} не найдено"}
        m = msgs
        if not m.buttons:
            return {"ok": False, "error": "у этого сообщения нет кнопок"}

        reply_box: dict[str, Any] = {}
        done = asyncio.Event()
        if wait_for_reply:
            from telethon import events

            @client.on(events.NewMessage(from_users=entity))
            async def _h_new(event):  # noqa: ANN001
                reply_box["message"] = event.message
                done.set()

            @client.on(events.MessageEdited(chats=entity))
            async def _h_edit(event):  # noqa: ANN001
                if event.message.id == message_id:
                    reply_box["message"] = event.message
                    done.set()

        try:
            await retry_on_flood(lambda: m.click(text=button_text))
        except ValueError:
            available = [b.text for row in m.buttons for b in row]
            return {"ok": False, "error": f"кнопка {button_text!r} не найдена", "available_buttons": available}

        result: dict[str, Any] = {"ok": True, "clicked": button_text}
        if wait_for_reply:
            try:
                await asyncio.wait_for(done.wait(), timeout=_BOT_WAIT_TIMEOUT_S)
                result["result"] = _format_bot_message(reply_box["message"])
            except TimeoutError:
                result["result"] = None
                result["note"] = f"не дождались ответа за {_BOT_WAIT_TIMEOUT_S}с"
        return result
    finally:
        await client.disconnect()


if __name__ == "__main__":
    mcp.run()
