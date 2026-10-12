"""Темы форум-групп: определение темы сообщения, фильтр по теме, ссылки t.me/c/…"""

import asyncio
from types import SimpleNamespace as NS

import pytest
from telethon.tl.types import Channel, MessageActionTopicCreate, MessageReplyHeader

from app import topics
from app.client import parse_chat
from app.common import ToolFailure
from conftest import message

FORUM = Channel(id=5, title="Forum", photo=None, date=None, megagroup=True, forum=True, access_hash=1)
GROUP = Channel(id=6, title="Group", photo=None, date=None, megagroup=True, access_hash=2)


def in_topic(mid, top, reply_to_msg_id=None):
    hdr = MessageReplyHeader(forum_topic=True, reply_to_msg_id=reply_to_msg_id or top,
                             reply_to_top_id=top if reply_to_msg_id else None)
    return message(mid, reply_to=hdr)


def test_topic_of():
    assert topics.topic_of(message(10, reply_to=None)) == topics.GENERAL_TOPIC_ID
    assert topics.topic_of(in_topic(11, 2)) == 2
    assert topics.topic_of(in_topic(12, 2, reply_to_msg_id=11)) == 2  # ответ внутри темы
    plain_reply = message(13, reply_to=MessageReplyHeader(reply_to_msg_id=10))
    assert topics.topic_of(plain_reply) == topics.GENERAL_TOPIC_ID
    assert topics.topic_of(NS(id=2, action=MessageActionTopicCreate(title="Лекции", icon_color=0))) == 2


class Client:
    def __init__(self, msgs):
        self.msgs, self.calls = msgs, []

    async def iter_messages(self, entity, **kw):
        self.calls.append(kw)
        for m in self.msgs:
            if kw.get("reply_to") and topics.topic_of(m) != kw["reply_to"]:
                continue
            yield m


def collect(client, entity, topic_id, **kw):
    async def go():
        return [m.id async for m in topics.iter_messages(client, entity, topic_id, **kw)]

    return asyncio.run(go())


def test_regular_topic_uses_server_filter():
    c = Client([in_topic(3, 2), message(4, reply_to=None), in_topic(5, 7)])
    assert collect(c, FORUM, 2, limit=10) == [3]
    assert c.calls == [{"reply_to": 2, "limit": 10}]


def test_general_and_search_filtered_locally_with_limit():
    msgs = [message(9, reply_to=None), in_topic(8, 2), message(7, reply_to=None), message(6, reply_to=None)]
    assert collect(Client(msgs), FORUM, 1, limit=2) == [9, 7]
    c = Client(msgs)
    assert collect(c, FORUM, 2, search="x", limit=5) == [8]
    assert "reply_to" not in c.calls[0]


def test_topic_requires_forum():
    with pytest.raises(ToolFailure) as exc:
        collect(Client([]), GROUP, 2)
    assert exc.value.code == "not_a_forum"
    assert collect(Client([message(1, reply_to=None)]), GROUP, None) == [1]


@pytest.mark.parametrize(
    "link", ["t.me/c/3250900186/2", "https://t.me/c/3250900186", "https://t.me/c/3250900186/2/905"]
)
def test_private_links_resolve_to_peer_id(link):
    assert parse_chat(link) == -1003250900186
