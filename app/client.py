"""Telethon client — подключение пользовательским Telegram-аккаунтом (не бот).

Сессия: локальный файл data/session.session (SQLite от Telethon), права 600,
никогда не коммитится (.gitignore). Хранится только на вашем компьютере.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient

log = logging.getLogger("tg-collector")

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
SESSION_PATH = DATA_DIR / "session"  # telethon добавит .session сам

load_dotenv(ROOT / ".env")

API_ID = os.environ.get("TELEGRAM_API_ID", "")
API_HASH = os.environ.get("TELEGRAM_API_HASH", "")


def _require_creds() -> tuple[int, str]:
    if not API_ID or not API_HASH:
        raise RuntimeError(
            "TELEGRAM_API_ID / TELEGRAM_API_HASH не заданы — заполните .env "
            "(получить на https://my.telegram.org/apps)"
        )
    return int(API_ID), API_HASH


def make_client() -> TelegramClient:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    api_id, api_hash = _require_creds()
    return TelegramClient(str(SESSION_PATH), api_id, api_hash)


def session_exists() -> bool:
    return SESSION_PATH.with_suffix(".session").exists()
