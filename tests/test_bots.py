"""Боты: только боты-получатели, корреляция ответов, снятие обработчиков (ревью, пп. 4, 6)."""

import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, patch

import app.bots as bots
import app.server as server
from conftest import BOT, HUMAN, message


def test_send_to_bot_refuses_non_bot():
    client = NS(send_message=AsyncMock())
    with patch.object(server.telegram, "get", AsyncMock(return_value=client)), \
         patch("app.client.resolve_entity", AsyncMock(return_value=HUMAN)):
        result = asyncio.run(server.send_to_bot(111, "hello", wait_for_reply=False))
    assert result["ok"] is False and result["code"] == "not_a_bot"
    client.send_message.assert_not_awaited()


class BotClient:
    """Бот отвечает на каждую команду с задержкой; до ответа присылает «фоновое» старое сообщение."""

    def __init__(self):
        self.handlers = []
        self.next_id = 100

    def add_event_handler(self, cb, builder):
        self.handlers.append(cb)

    def remove_event_handler(self, cb):
        self.handlers = [h for h in self.handlers if h != cb]

    async def _emit(self, m):
        for h in list(self.handlers):
            await h(NS(message=m))

    async def send_message(self, entity, text):
        self.next_id += 1
        sent = message(self.next_id, text)
        command_id = sent.id

        async def reply():
            await asyncio.sleep(0.01)
            await self._emit(message(command_id - 50, "old background message"))
            self.next_id += 1
            await self._emit(message(self.next_id, f"reply to {text}", reply_to_msg_id=command_id))

        asyncio.get_running_loop().create_task(reply())
        return sent


def test_concurrent_commands_get_their_own_replies():
    client = BotClient()

    async def run():
        return await asyncio.gather(
            bots.send_command(client, BOT, "FIRST", True), bots.send_command(client, BOT, "SECOND", True)
        )

    first, second = asyncio.run(run())
    assert first["reply"]["text"] == "reply to FIRST"
    assert second["reply"]["text"] == "reply to SECOND"
    assert client.handlers == []  # обработчики сняты
