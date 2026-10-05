"""MCP-сервер сбора данных из Telegram: группы/каналы (история, поиск, участники, выгрузка)
и работа с ботами (отправить команду, нажать кнопку, прочитать ответ).

Один Telegram-аккаунт пользователя (Telethon, data/session.session), один клиент на процесс
(TelegramService) с межпроцессной блокировкой сессии. Присоединение к новым каналам — явное
действие инструмента join_channel, каждый раз логируется.

Ошибки, которые можно исправить (не найдено, неверный аргумент, FloodWait, сессия занята),
инструменты возвращают единым словарём {"ok": false, "code": ..., "error": ..., ...};
для FloodWait — с retry_after в секундах.

Запуск (из .mcp.json хост-процессом Claude Code):
    python -m app.server
"""

from __future__ import annotations

import functools
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from telethon.errors import (
    ChannelPrivateError,
    ChannelsTooMuchError,
    ChatAdminRequiredError,
    FloodWaitError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    InviteRequestSentError,
    UserAlreadyParticipantError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.tl.types import Channel, Chat, User
from telethon.utils import get_peer_id

from app import bots
from app.client import TelegramService, resolve_bot, resolve_entity
from app.common import ToolFailure, check_range, iso, parse_range
from app.export import DEFAULT_MAX_MB, DEFAULT_MAX_WORDS, ExportManager, list_jobs
from app.flood import retry_on_flood
from app.media import MEDIA_DIR, download_message_media

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tg-collector")

telegram = TelegramService()
exports = ExportManager(telegram)


@asynccontextmanager
async def _lifespan(_server: FastMCP) -> AsyncIterator[None]:
    try:
        yield
    finally:
        await exports.shutdown()  # задания сохранят статус interrupted — их можно продолжить
        await telegram.close()


mcp = FastMCP("tg-collector", lifespan=_lifespan)

_MAX_MEDIA_PER_CALL = 50
_MAX_MEDIA_SIZE_MB = 1536  # 1,5 ГБ
_MAX_EXPORT_WAIT_S = 50


def _tool(*, read_only: bool, destructive: bool = False, idempotent: bool = False) -> Callable:
    """@mcp.tool с аннотациями побочных эффектов и единым форматом ожидаемых ошибок."""

    def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return await fn(*args, **kwargs)
            except ToolFailure as e:
                return e.as_dict()
            except FloodWaitError as e:
                return {
                    "ok": False,
                    "code": "flood_wait",
                    "error": f"Telegram ограничил частоту запросов — повторите через {e.seconds} с",
                    "retry_after": e.seconds,
                }

        annotations = ToolAnnotations(
            readOnlyHint=read_only,
            destructiveHint=destructive,
            idempotentHint=idempotent,
            openWorldHint=True,
        )
        return mcp.tool(annotations=annotations)(wrapper)

    return decorator


def _entity_kind(e: Any) -> str:
    if isinstance(e, User):
        return "bot" if e.bot else "user"
    if isinstance(e, Channel):
        return "channel" if e.broadcast else "supergroup"
    if isinstance(e, Chat):
        return "group"
    return "unknown"


def _message_row(m: Any, with_sender: bool = True) -> dict:
    row: dict[str, Any] = {"id": m.id, "date": iso(m.date), "sender_id": m.sender_id}
    if with_sender:
        s = m.sender
        row["sender"] = (getattr(s, "username", None) or getattr(s, "first_name", None)) if s else None
    row.update(
        text=m.message or None,
        has_media=m.media is not None,
        media_type=type(m.media).__name__ if m.media else None,
    )
    return row


@_tool(read_only=True, idempotent=True)
async def list_dialogs(limit: int = 50) -> list[dict] | dict:
    """Список чатов аккаунта: личные, группы, каналы, боты — с id и названием.

    Используйте, чтобы узнать точный chat_id/username перед вызовом остальных инструментов.
    limit — от 1 до 1000.
    """
    check_range("limit", limit, 1, 1000)
    client = await telegram.get()
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


@_tool(read_only=False, idempotent=True)
async def join_channel(link_or_username: str) -> dict:
    """Присоединиться к публичному каналу/группе по @username или ссылке-приглашению (t.me/+...).

    Действие видимое и необратимое без выхода из канала — каждый вызов логируется.
    После этого канал станет доступен в list_dialogs/get_history/search_messages.
    """
    client = await telegram.get()
    link = link_or_username.strip().split("?")[0].rstrip("/")
    try:
        if "joinchat/" in link or "/+" in link or link.startswith("+"):
            invite_hash = link.split("/")[-1].lstrip("+")
            try:
                result = await retry_on_flood(lambda: client(ImportChatInviteRequest(invite_hash)))
                chat, already = result.chats[0], False
            except UserAlreadyParticipantError:
                # по хэшу ссылки сущность не найти — спрашиваем Telegram, что за чат
                invite = await retry_on_flood(lambda: client(CheckChatInviteRequest(invite_hash)))
                chat, already = getattr(invite, "chat", None), True
                if chat is None:
                    return {"ok": True, "already_member": True, "id": None, "title": getattr(invite, "title", None)}
        else:
            chat = await resolve_entity(client, link.split("t.me/")[-1].lstrip("@"))
            if not isinstance(chat, Channel):
                raise ToolFailure("not_a_channel", "это не канал/группа (пользователь или бот)")
            already = not chat.left
            if not already:
                await retry_on_flood(lambda: client(JoinChannelRequest(chat)))
    except InviteRequestSentError:
        raise ToolFailure("pending", "заявка на вступление отправлена — ждёт одобрения админа", pending=True) from None
    except (InviteHashExpiredError, InviteHashInvalidError):
        raise ToolFailure("invalid_invite", "пригласительная ссылка недействительна или истекла") from None
    except ChannelsTooMuchError:
        raise ToolFailure("too_many_channels", "превышен лимит каналов на аккаунт (слишком много подписок)") from None
    except ChannelPrivateError:
        raise ToolFailure("private", "канал приватный — нужна действующая пригласительная ссылка") from None
    if not already:
        log.info("join_channel: joined %r (id=%s)", getattr(chat, "title", link), chat.id)
    # id в том же формате, что в list_dialogs (-100…) — его принимают остальные инструменты
    out = {"ok": True, "id": get_peer_id(chat), "title": getattr(chat, "title", None)}
    if already:
        out["already_member"] = True
    return out


@_tool(read_only=True, idempotent=True)
async def get_history(
    chat: str | int,
    since: str | None = None,
    until: str | None = None,
    limit: int = 200,
    before_id: int | None = None,
) -> list[dict] | dict:
    """История сообщений чата за период [since, until), от старых к новым.

    chat — id, @username или ссылка t.me/...
    since/until — ISO-даты ("2026-09-01" или "2026-09-01T00:00:00"), можно опустить любую;
    since включительно, until — нет.
    limit — от 1 до 5000 (защита от случайной выгрузки всей истории разом; для всего чата —
    export_chat_md).
    before_id — продолжение: только сообщения с id меньше этого (передайте наименьший id
    из предыдущего ответа, чтобы получить более старые).
    """
    check_range("limit", limit, 1, 5000)
    since_dt, until_dt = parse_range(since, until)
    client = await telegram.get()
    entity = await resolve_entity(client, chat)
    out = []
    kwargs: dict[str, Any] = {"limit": limit, "offset_date": until_dt}
    if before_id:
        kwargs["offset_id"] = before_id
    async for m in client.iter_messages(entity, **kwargs):
        if since_dt and m.date and m.date < since_dt:
            break
        out.append(_message_row(m))
    out.reverse()
    return out


@_tool(read_only=True, idempotent=True)
async def search_messages(
    chat: str | int, query: str, limit: int = 100, before_id: int | None = None
) -> list[dict] | dict:
    """Поиск сообщений по ключевым словам внутри одного чата (серверный поиск Telegram).

    limit — от 1 до 1000. before_id — продолжение: только сообщения с id меньше этого
    (наименьший id из предыдущего ответа).
    """
    check_range("limit", limit, 1, 1000)
    client = await telegram.get()
    entity = await resolve_entity(client, chat)
    kwargs: dict[str, Any] = {"search": query, "limit": limit}
    if before_id:
        kwargs["offset_id"] = before_id
    return [_message_row(m, with_sender=False) async for m in client.iter_messages(entity, **kwargs)]


@_tool(read_only=False)
async def export_chat_md(
    chat: str | int,
    since: str | None = None,
    until: str | None = None,
    max_words: int = DEFAULT_MAX_WORDS,
    max_mb: float = DEFAULT_MAX_MB,
    by_month: bool = False,
    wait_seconds: int = 20,
) -> dict:
    """Выгрузить историю чата в Markdown-файлы на диск, разбив на части. Работает в фоне.

    Готовый результат: data/exports/<chat_id>/<job_id>/<NNN>_<первая дата>_<последняя дата>.md
    плюс manifest.json. Пока выгрузка не завершена, наружу ничего не публикуется.
    chat — id, @username или ссылка t.me/...
    since/until — ISO-даты интервала [since, until), можно опустить (тогда весь чат).
    max_words/max_mb — лимит на одну часть вместе с заголовком; по умолчанию 400 тыс. слов
    и 10 МБ — подходит для NotebookLM (до 500 тыс. слов на источник) и Perplexity.
    Сообщение больше лимита идёт отдельной частью и попадает в oversized_parts.
    by_month — дополнительно начинать новую часть с каждого месяца.
    wait_seconds — сколько ждать завершения в этом вызове (0–50). Если не успело —
    вернётся status=running и job_id: проверяйте export_status(job_id).
    Возвращает пути к файлам, а не их содержимое — читайте их через Read.
    Медиа не скачиваются: в файле есть строка 📎 с id сообщения для download_media.
    """
    check_range("wait_seconds", wait_seconds, 0, _MAX_EXPORT_WAIT_S)
    return await exports.start(
        wait_seconds,
        chat=chat,
        since=since,
        until=until,
        max_words=max_words,
        max_mb=max_mb,
        by_month=by_month,
    )


@_tool(read_only=True, idempotent=True)
async def export_status(job_id: str | None = None) -> dict:
    """Статус задания выгрузки (running, waiting_flood, paused, interrupted, cancelled, failed,
    complete) и, когда готово, пути к файлам. Без job_id — список последних заданий."""
    if job_id is None:
        return {"ok": True, "jobs": list_jobs()}
    return exports.status(job_id)


@_tool(read_only=False)
async def export_resume(job_id: str, wait_seconds: int = 20) -> dict:
    """Продолжить приостановленную (FloodWait), прерванную, отменённую или упавшую выгрузку
    с последней готовой части — без повторной обработки уже выгруженных сообщений."""
    check_range("wait_seconds", wait_seconds, 0, _MAX_EXPORT_WAIT_S)
    return await exports.resume(job_id, wait_seconds)


@_tool(read_only=False)
async def export_cancel(job_id: str) -> dict:
    """Отменить выполняющуюся выгрузку. Готовые части сохраняются — её можно продолжить export_resume."""
    return await exports.cancel(job_id)


@_tool(read_only=False, idempotent=True)
async def download_media(
    chat: str | int, message_ids: list[int], max_size_mb: int = _MAX_MEDIA_SIZE_MB
) -> list[dict] | dict:
    """Скачать фото/видео/аудио/файлы из указанных сообщений чата на диск (data/media/<chat_id>/).

    message_ids берите из get_history/search_messages (поле has_media = true).
    Возвращает локальный путь к файлу — его можно открыть через Read (картинки видны).
    Файлы больше max_size_mb пропускаются, не более 50 сообщений за вызов.
    Повторный запрос берёт файл из кэша, если вложение в Telegram не менялось.
    """
    if not message_ids:
        return []
    check_range("количество message_ids", len(message_ids), 1, _MAX_MEDIA_PER_CALL)
    check_range("max_size_mb", max_size_mb, 1, _MAX_MEDIA_SIZE_MB)
    client = await telegram.get()
    entity = await resolve_entity(client, chat)
    target_dir = MEDIA_DIR / str(get_peer_id(entity))
    target_dir.mkdir(parents=True, exist_ok=True)
    msgs = await retry_on_flood(lambda: client.get_messages(entity, ids=message_ids))
    out: list[dict] = []
    for mid, m in zip(message_ids, msgs):
        try:
            if m is None:
                raise ToolFailure("not_found", "сообщение не найдено")
            if not m.media:
                raise ToolFailure("no_media", "в сообщении нет медиа")
            size = getattr(getattr(m, "file", None), "size", None)
            if size is None:
                raise ToolFailure("unknown_size", "размер файла неизвестен (превью ссылки или нестандартное медиа)")
            if size > max_size_mb * 1024 * 1024:
                raise ToolFailure("too_large", f"файл {size // 1024 // 1024} МБ больше лимита {max_size_mb} МБ")
            path, cached = await download_message_media(client, m, target_dir, size)
            out.append(
                {"id": mid, "ok": True, "path": str(path), "mime": m.file.mime_type, "size": size, "cached": cached}
            )
        except ToolFailure as e:
            out.append({"id": mid, **e.as_dict()})
        except FloodWaitError as e:
            out.append({"id": mid, "ok": False, "code": "flood_wait", "error": "FloodWait", "retry_after": e.seconds})
        except Exception as e:  # noqa: BLE001 — один сбойный файл не должен терять остальные
            log.warning("download_media: msg=%s failed: %r", mid, e)
            out.append({"id": mid, "ok": False, "code": "download_failed", "error": f"{type(e).__name__}: {e}"})
    return out


@_tool(read_only=True, idempotent=True)
async def list_participants(chat: str | int, limit: int = 500) -> list[dict] | dict:
    """Список участников группы/канала (если список открыт — многие каналы его скрывают).

    limit — от 1 до 10000.
    """
    check_range("limit", limit, 1, 10_000)
    client = await telegram.get()
    entity = await resolve_entity(client, chat)
    try:
        return [
            {
                "id": u.id,
                "username": u.username,
                "first_name": u.first_name,
                "last_name": u.last_name,
                "is_bot": u.bot,
            }
            async for u in client.iter_participants(entity, limit=limit)
        ]
    except ChatAdminRequiredError:
        raise ToolFailure("hidden", "список участников скрыт — нужны права администратора") from None


@_tool(read_only=False)
async def send_to_bot(bot: str | int, text: str, wait_for_reply: bool = True) -> dict:
    """Отправить сообщение/команду боту (например /start) и дождаться его ответа.

    Только для ботов: людям и в группы инструмент не пишет (code=not_a_bot).
    Команды одному боту выполняются по очереди; ответом считается сообщение бота после
    этой команды. Возвращает текст ответа и кнопки (если есть), чтобы следующим шагом
    можно было позвать click_button с нужной подписью кнопки.
    """
    client = await telegram.get()
    entity = await resolve_bot(client, bot)
    result = await bots.send_command(client, entity, text, wait_for_reply)
    result["bot"] = bot
    return result


@_tool(read_only=True, idempotent=True)
async def get_recent_bot_messages(bot: str | int, limit: int = 10) -> list[dict] | dict:
    """Последние сообщения в диалоге с ботом — посмотреть состояние перед click_button. limit — от 1 до 100."""
    check_range("limit", limit, 1, 100)
    client = await telegram.get()
    entity = await resolve_bot(client, bot)
    return [bots.format_bot_message(m) async for m in client.iter_messages(entity, limit=limit)]


@_tool(read_only=False)
async def click_button(bot: str | int, message_id: int, button_text: str, wait_for_reply: bool = True) -> dict:
    """Нажать inline-кнопку на конкретном сообщении бота (по видимому тексту кнопки).

    message_id берите из send_to_bot / get_recent_bot_messages.
    Возвращает результат нажатия: новое/изменённое сообщение бота, если дождались.
    """
    client = await telegram.get()
    entity = await resolve_bot(client, bot)
    return await bots.click(client, entity, message_id, button_text, wait_for_reply)


if __name__ == "__main__":
    mcp.run()
