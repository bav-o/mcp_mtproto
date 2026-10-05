"""Диалог с ботом: отправка команды / нажатие кнопки и ожидание именно своего ответа.

Операции с одним ботом сериализуются (одна команда — один ответ). Ответом считается сообщение
бота, пришедшее после нашей команды (id больше), и если бот указал reply_to — то в ответ именно
на неё. Telegram не гарантирует reply_to у каждого бота, поэтому основную корреляцию даёт
сериализация. Обработчики событий снимаются в finally — клиент живёт весь процесс.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from telethon import TelegramClient, events

from app.common import KeyedLocks, ToolFailure, iso
from app.flood import retry_on_flood

BOT_WAIT_TIMEOUT_S = 20
_bot_locks = KeyedLocks()


def format_bot_message(m: Any) -> dict:
    buttons = []
    if m.buttons:
        for row_i, row in enumerate(m.buttons):
            for col_i, b in enumerate(row):
                buttons.append({"row": row_i, "col": col_i, "text": b.text})
    return {"message_id": m.id, "date": iso(m.date), "text": m.message or None, "buttons": buttons}


class _EventCollector:
    """Складывает события в очередь с момента регистрации — ответ не потеряется, даже если
    придёт раньше, чем вернётся send_message."""

    def __init__(self, client: TelegramClient, builders: list[Any]) -> None:
        self.client, self.builders = client, builders
        self.queue: asyncio.Queue[Any] = asyncio.Queue()

    async def _handle(self, event: Any) -> None:
        self.queue.put_nowait(event)

    async def __aenter__(self) -> _EventCollector:
        for b in self.builders:
            self.client.add_event_handler(self._handle, b)
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.client.remove_event_handler(self._handle)

    async def wait_for(self, predicate: Callable[[Any], bool], timeout: float) -> Any | None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while (remaining := deadline - loop.time()) > 0:
            try:
                event = await asyncio.wait_for(self.queue.get(), remaining)
            except TimeoutError:
                return None
            if predicate(event):
                return event
        return None


def _answers(m: Any, command_id: int) -> bool:
    reply_to = getattr(m, "reply_to_msg_id", None)
    return m.id > command_id and (reply_to is None or reply_to == command_id)


async def send_command(client: TelegramClient, bot: Any, text: str, wait_for_reply: bool) -> dict:
    async with _bot_locks.hold(bot.id):
        result: dict[str, Any] = {"ok": True, "sent": text}
        if not wait_for_reply:
            await retry_on_flood(lambda: client.send_message(bot, text))
            return result
        async with _EventCollector(client, [events.NewMessage(from_users=bot)]) as col:
            sent = await retry_on_flood(lambda: client.send_message(bot, text))
            result["sent_message_id"] = sent.id
            event = await col.wait_for(lambda ev: _answers(ev.message, sent.id), BOT_WAIT_TIMEOUT_S)
        if event is None:
            result["reply"] = None
            result["note"] = f"бот не ответил за {BOT_WAIT_TIMEOUT_S}с"
        else:
            result["reply"] = format_bot_message(event.message)
        return result


async def click(client: TelegramClient, bot: Any, message_id: int, button_text: str, wait_for_reply: bool) -> dict:
    async with _bot_locks.hold(bot.id):
        m = await retry_on_flood(lambda: client.get_messages(bot, ids=message_id))
        if not m:
            raise ToolFailure("not_found", f"сообщение {message_id} не найдено")
        if not m.buttons:
            raise ToolFailure("no_buttons", "у этого сообщения нет кнопок")

        # ищем кнопку сами: Message.click(text=...) при промахе молча возвращает None
        flat = [(i, j, b) for i, row in enumerate(m.buttons) for j, b in enumerate(row)]
        match = next((x for x in flat if x[2].text == button_text), None) or next(
            (x for x in flat if x[2].text.strip().casefold() == button_text.strip().casefold()), None
        )
        if match is None:
            raise ToolFailure(
                "button_not_found",
                f"кнопка {button_text!r} не найдена",
                available_buttons=[b.text for _, _, b in flat],
            )
        row_i, col_i, button = match

        # новые сообщения бота после нажатия — только с id больше последнего уже существующего
        latest = await retry_on_flood(lambda: client.get_messages(bot, limit=1))
        last_id = max([message_id, *(x.id for x in latest or [])])

        def is_result(ev: Any) -> bool:
            if isinstance(ev, events.MessageEdited.Event):  # подкласс NewMessage.Event — проверять первым
                return ev.message.id == message_id
            return ev.message.id > last_id

        result: dict[str, Any] = {"ok": True, "clicked": button.text}
        builders = [events.NewMessage(from_users=bot), events.MessageEdited(chats=bot)] if wait_for_reply else []
        async with _EventCollector(client, builders) as col:
            answer = await retry_on_flood(lambda: m.click(row_i, col_i))
            if isinstance(answer, str):  # URL-кнопка: Telethon не открывает ссылку, а возвращает её
                result["url"] = answer
                return result
            if getattr(answer, "message", None):  # всплывающее уведомление/alert от бота
                result["bot_answer"] = answer.message
            if not wait_for_reply:
                return result
            event = await col.wait_for(is_result, BOT_WAIT_TIMEOUT_S)
        if event is None:
            result["result"] = None
            result["note"] = f"не дождались ответа за {BOT_WAIT_TIMEOUT_S}с"
        else:
            result["result"] = format_bot_message(event.message)
        return result
