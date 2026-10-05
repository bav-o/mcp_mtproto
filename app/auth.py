"""Разовый интерактивный вход — запускать руками один раз.

Спросит номер телефона вашего Telegram-аккаунта и код из Telegram (и пароль 2FA, если включён).
После успеха создаст data/session.session — дальше MCP-сервер использует его молча.

Запуск:
    python -m app.auth   (macOS/Linux: .venv/bin/python, Windows: .venv\Scripts\python.exe)
"""

from __future__ import annotations

import asyncio
import os

from app.client import SESSION_PATH, make_client


async def _login() -> None:
    client = make_client()
    await client.start()  # сам спросит телефон/код/2FA в терминале
    me = await client.get_me()
    print(f"\nВход выполнен: {me.first_name} (@{me.username or '—'}), id={me.id}")
    print(f"Сессия сохранена: {SESSION_PATH}.session")
    await client.disconnect()
    try:  # файл сесії = повний доступ до акаунта; на Windows chmod ігнорується
        os.chmod(f"{SESSION_PATH}.session", 0o600)
    except OSError:
        pass


if __name__ == "__main__":
    asyncio.run(_login())
