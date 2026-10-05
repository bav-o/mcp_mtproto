"""Разовый интерактивный вход — запускать руками один раз.

Спросит номер телефона вашего Telegram-аккаунта и код из Telegram (и пароль 2FA, если включён).
После успеха создаст data/session.session — дальше MCP-сервер использует его молча.
Папка data/ создаётся с правами 700, файл сессии — 600 до того, как в него что-то запишется.
Пока MCP-сервер запущен, вход не выполнится: сессией владеет один процесс.

Запуск:
    python -m app.auth   (macOS/Linux: .venv/bin/python, Windows: .venv\\Scripts\\python.exe)
"""

from __future__ import annotations

import asyncio

from app.client import SESSION_FILE, TelegramService
from app.common import ToolFailure


async def _login() -> None:
    try:
        async with TelegramService(require_auth=False, create_session=True) as client:
            await client.start()  # сам спросит телефон/код/2FA в терминале
            me = await client.get_me()
            print(f"\nВход выполнен: {me.first_name} (@{me.username or '—'}), id={me.id}")
            print(f"Сессия сохранена: {SESSION_FILE}")
    except ToolFailure as e:
        raise SystemExit(e.message) from None


if __name__ == "__main__":
    asyncio.run(_login())
