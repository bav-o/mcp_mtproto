"""Telethon client — подключение пользовательским Telegram-аккаунтом (не бот).

Сессия: локальный файл data/session.session (SQLite от Telethon), никогда не коммитится
(.gitignore). На POSIX папка data/ создаётся с правами 700, файл сессии — 600 ещё до того,
как Telethon запишет в него ключ авторизации.

Владелец сессии один: TelegramService держит единственный TelegramClient на процесс и
межпроцессную блокировку data/session.lock. Второй процесс (ещё один MCP-хост, CLI-выгрузка
при запущенном сервере) получает понятную ошибку session_busy вместо «database is locked».
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import suppress
from pathlib import Path
from typing import IO, Any

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.types import User

from app.common import ToolFailure
from app.flood import retry_on_flood

log = logging.getLogger("tg-collector")

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("MTPROTO_DATA_DIR") or ROOT / "data")
SESSION_PATH = DATA_DIR / "session"  # telethon добавит .session сам
SESSION_FILE = SESSION_PATH.with_suffix(".session")
LOCK_FILE = DATA_DIR / "session.lock"

load_dotenv(ROOT / ".env")


def _require_creds() -> tuple[int, str]:
    api_id = os.environ.get("TELEGRAM_API_ID", "")
    api_hash = os.environ.get("TELEGRAM_API_HASH", "")
    if not api_id or not api_hash:
        raise ToolFailure(
            "no_credentials",
            "TELEGRAM_API_ID / TELEGRAM_API_HASH не заданы — заполните .env "
            "(получить на https://my.telegram.org/apps)",
        )
    return int(api_id), api_hash


def session_exists() -> bool:
    return SESSION_FILE.exists()


def prepare_storage(create_session: bool = False) -> None:
    """Закрытая папка data/ и файл сессии 600 — до того, как Telethon что-то в него запишет.

    На Windows chmod не управляет доступом (там ACL): data/ внутри профиля пользователя
    по умолчанию доступна только ему.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    if os.name == "nt":
        if create_session and not SESSION_FILE.exists():
            SESSION_FILE.touch()
        return
    if DATA_DIR.stat().st_mode & 0o077:
        os.chmod(DATA_DIR, 0o700)
    if create_session and not SESSION_FILE.exists():
        os.close(os.open(SESSION_FILE, os.O_CREAT | os.O_WRONLY, 0o600))  # пустой файл = пустая SQLite-база
    for f in (SESSION_FILE, SESSION_FILE.with_name(SESSION_FILE.name + "-journal")):
        if f.exists() and f.stat().st_mode & 0o077:
            log.warning("права %s были шире 600 — исправляю", f)
            os.chmod(f, 0o600)


class SessionLock:
    """Межпроцессная эксклюзивная блокировка файла сессии (flock на POSIX, msvcrt на Windows)."""

    def __init__(self, path: Path = LOCK_FILE) -> None:
        self.path = path
        self._fh: IO[str] | None = None

    def acquire(self) -> None:
        if self._fh is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fh = os.fdopen(os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600), "r+")
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            holder = ""
            with suppress(OSError):
                holder = fh.read().strip()
            fh.close()
            raise ToolFailure(
                "session_busy",
                f"Сессия Telegram занята другим процессом ({holder or 'pid неизвестен'}): "
                "уже запущен MCP-сервер в другом приложении или выгрузка в терминале. "
                "Закройте его и повторите.",
            ) from None
        if os.name != "nt":  # на Windows заблокированный байт нельзя переписывать
            fh.seek(0)
            fh.truncate()
            fh.write(f"pid {os.getpid()}")
            fh.flush()
        self._fh = fh

    def release(self) -> None:
        fh, self._fh = self._fh, None
        if fh is None:
            return
        with suppress(OSError):
            if os.name == "nt":
                import msvcrt

                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()


class TelegramService:
    """Единственный TelegramClient процесса: создаётся лениво, переподключается, закрывается в close()."""

    def __init__(self, *, require_auth: bool = True, create_session: bool = False) -> None:
        self._require_auth = require_auth
        self._create_session = create_session
        self._client: TelegramClient | None = None
        self._lock = asyncio.Lock()
        self._session_lock = SessionLock()

    async def get(self) -> TelegramClient:
        async with self._lock:
            if self._client is not None:
                if not self._client.is_connected():
                    await self._client.connect()
                return self._client
            if not self._create_session and not session_exists():
                raise ToolFailure(
                    "no_session",
                    "Сессия не найдена. Сначала выполните вход: "
                    ".venv/bin/python -m app.auth (один раз, руками, спросит код из Telegram).",
                )
            creds = _require_creds()
            self._session_lock.acquire()
            client: TelegramClient | None = None
            try:
                prepare_storage(self._create_session)
                client = TelegramClient(str(SESSION_PATH), *creds)
                await client.connect()
                if self._require_auth and not await retry_on_flood(client.is_user_authorized):
                    raise ToolFailure("session_expired", "Сессия протухла или отозвана — повторите вход через app.auth")
            except BaseException:  # включая отмену: соединение и блокировка не должны утечь
                if client is not None:
                    with suppress(Exception):
                        await client.disconnect()
                self._session_lock.release()
                raise
            self._client = client
            return client

    async def close(self) -> None:
        async with self._lock:
            client, self._client = self._client, None
            try:
                if client is not None:
                    await client.disconnect()
            finally:
                self._session_lock.release()

    async def __aenter__(self) -> TelegramClient:
        return await self.get()

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


def parse_chat(chat: str | int) -> str | int:
    """Строку из цифр ("-100123…") превращает в int: иначе Telethon ищет её как username/телефон."""
    if isinstance(chat, str) and chat.strip().lstrip("-").isdigit():
        return int(chat.strip())
    return chat


async def resolve_entity(client: TelegramClient, chat: str | int) -> Any:
    """get_entity с поддержкой числового id, которого ещё нет в кэше сессии."""
    peer = parse_chat(chat)
    try:
        try:
            return await retry_on_flood(lambda: client.get_entity(peer))
        except ValueError:
            if not isinstance(peer, int):
                raise
            # по голому id Telethon находит только закэшированные сущности —
            # загружаем диалоги (они кэшируют сущности в сессии) и пробуем снова
            await retry_on_flood(lambda: client.get_dialogs())
            return await retry_on_flood(lambda: client.get_entity(peer))
    except (ValueError, UsernameInvalidError, UsernameNotOccupiedError):
        raise ToolFailure("not_found", f"чат {chat!r} не найден — проверьте id через list_dialogs или @username") from None


async def resolve_bot(client: TelegramClient, bot: str | int) -> User:
    """Как resolve_entity, но только бот: инструменты для ботов не пишут людям и в группы."""
    entity = await resolve_entity(client, bot)
    if not (isinstance(entity, User) and entity.bot):
        raise ToolFailure("not_a_bot", f"{bot!r} — не бот; инструменты для ботов работают только с ботами")
    return entity
