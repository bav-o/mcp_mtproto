"""Темы (вкладки) форум-групп: к какой теме относится сообщение, список тем, сообщения одной темы.

Id темы = id служебного сообщения о её создании. Тема «General» (id 1) не создаётся сообщением:
в неё попадает всё, что не помечено forum_topic. Telegram не отдаёт её через messages.getReplies,
поэтому её сообщения фильтруются на нашей стороне.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from telethon import TelegramClient
from telethon.tl.functions.messages import GetForumTopicsRequest, GetRepliesRequest
from telethon.tl.types import Channel, ForumTopic, MessageActionTopicCreate

from app.common import ToolFailure, iso
from app.flood import retry_on_flood

GENERAL_TOPIC_ID = 1
_PAGE = 100


def is_forum(entity: Any) -> bool:
    return isinstance(entity, Channel) and bool(getattr(entity, "forum", False))


def require_forum(entity: Any) -> None:
    if not is_forum(entity):
        raise ToolFailure("not_a_forum", "в этом чате нет тем (вкладок) — topic_id не нужен")


def topic_of(m: Any) -> int:
    """Id темы сообщения форум-группы."""
    if isinstance(getattr(m, "action", None), MessageActionTopicCreate):
        return m.id
    r = getattr(m, "reply_to", None)
    if r is not None and getattr(r, "forum_topic", False):
        return r.reply_to_top_id or r.reply_to_msg_id
    return GENERAL_TOPIC_ID


async def iter_messages(client: TelegramClient, entity: Any, topic_id: int | None, **kwargs: Any) -> AsyncIterator[Any]:
    """client.iter_messages с фильтром по теме (None — все темы).

    Обычная тема — серверный фильтр (getReplies). Поиск (search) getReplies не умеет, а General
    через него не отдаётся: тогда перебираем весь чат и отбираем сообщения темы сами, сохраняя limit.
    """
    if topic_id is None:
        async for m in client.iter_messages(entity, **kwargs):
            yield m
        return
    require_forum(entity)
    if topic_id != GENERAL_TOPIC_ID and "search" not in kwargs:
        async for m in client.iter_messages(entity, reply_to=topic_id, **kwargs):
            yield m
        return
    limit = kwargs.pop("limit", None)
    found = 0
    async for m in client.iter_messages(entity, limit=None, **kwargs):
        if topic_of(m) != topic_id:
            continue
        yield m
        found += 1
        if limit is not None and found >= limit:
            return


async def fetch_topics(client: TelegramClient, entity: Any) -> list[ForumTopic]:
    """Все темы форума (удалённые пропускаются), в порядке Telegram: закреплённые, затем по активности."""
    require_forum(entity)
    out: list[ForumTopic] = []
    offset_date, offset_id, offset_topic = None, 0, 0
    while True:
        res = await retry_on_flood(
            lambda: client(
                GetForumTopicsRequest(
                    entity, offset_date=offset_date, offset_id=offset_id, offset_topic=offset_topic, limit=_PAGE
                )
            )
        )
        page = [t for t in res.topics if isinstance(t, ForumTopic)]
        out.extend(page)
        if len(res.topics) < _PAGE or not page:
            return out
        last = page[-1]
        offset_date, offset_id, offset_topic = last.date, last.top_message, last.id


async def topic_title(client: TelegramClient, entity: Any, topic_id: int) -> str:
    for t in await fetch_topics(client, entity):
        if t.id == topic_id:
            return t.title
    raise ToolFailure("not_found", f"тема {topic_id} не найдена — список тем: list_topics")


async def count_messages(client: TelegramClient, entity: Any, topic_id: int) -> int | None:
    """Сколько сообщений в теме (как в шапке вкладки). Для General Telegram счётчик не отдаёт."""
    if topic_id == GENERAL_TOPIC_ID:
        return None
    res = await retry_on_flood(
        lambda: client(
            GetRepliesRequest(
                entity, msg_id=topic_id, offset_id=0, offset_date=None, add_offset=0, limit=1, max_id=0, min_id=0, hash=0
            )
        )
    )
    return getattr(res, "count", None) or len(res.messages)


def topic_row(t: ForumTopic) -> dict:
    return {
        "id": t.id,
        "title": t.title,
        "last_message_id": t.top_message,
        "date": iso(t.date),
        "pinned": bool(t.pinned),
        "closed": bool(t.closed),
        "hidden": bool(t.hidden),
        "unread_count": t.unread_count,
    }
