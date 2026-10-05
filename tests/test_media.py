"""Загрузка медиа: гонки, неполные файлы, замена вложения (ревью, пп. 3, 13)."""

import asyncio
from pathlib import Path
from types import SimpleNamespace as NS

import pytest
from telethon.tl.types import Document, MessageMediaDocument

import app.media as media
from app.common import ToolFailure
from conftest import message


def doc_message(mid=1, doc_id=10, size=6):
    doc = Document(id=doc_id, access_hash=0, file_reference=b"", date=None, mime_type="application/pdf",
                   size=size, dc_id=1, attributes=[])
    m = message(mid, media=MessageMediaDocument(document=doc))
    m.file = NS(size=size, ext=".pdf", mime_type="application/pdf")
    return m


class FakeClient:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = 0

    async def download_media(self, m, file):
        self.calls += 1
        payload = self.payloads.pop(0)
        with open(file, "wb") as fh:
            for chunk in payload:
                if isinstance(chunk, BaseException):
                    raise chunk
                fh.write(chunk)
                await asyncio.sleep(0)
        return file


def test_concurrent_same_message_downloads_once(tmp_path):
    client = FakeClient([[b"AAA", b"BBB"], [b"XXXXXX"]])
    m = doc_message()

    async def run():
        return await asyncio.gather(*(media.download_message_media(client, m, tmp_path, 6) for _ in range(2)))

    (p1, c1), (p2, c2) = asyncio.run(run())
    assert p1 == p2 and p1.read_bytes() == b"AAABBB"
    assert client.calls == 1 and {c1, c2} == {False, True}
    assert not list(tmp_path.glob("*.part"))


def test_failed_download_publishes_nothing(tmp_path):
    client = FakeClient([[b"A", RuntimeError("network")]])
    with pytest.raises(RuntimeError):
        asyncio.run(media.download_message_media(client, doc_message(), tmp_path, 6))
    assert sorted(p.name for p in tmp_path.iterdir()) == []


def test_size_mismatch_is_rejected(tmp_path):
    client = FakeClient([[b"AAA"]])
    with pytest.raises(ToolFailure) as err:
        asyncio.run(media.download_message_media(client, doc_message(), tmp_path, 6))
    assert err.value.code == "incomplete_download"
    assert sorted(p.name for p in tmp_path.iterdir()) == []


def test_cache_invalidated_when_attachment_replaced(tmp_path):
    client = FakeClient([[b"OLDOLD"], [b"NEWNEW"]])
    first, _ = asyncio.run(media.download_message_media(client, doc_message(doc_id=10), tmp_path, 6))
    again, cached = asyncio.run(media.download_message_media(client, doc_message(doc_id=10), tmp_path, 6))
    assert cached and again.read_bytes() == b"OLDOLD"
    replaced, cached = asyncio.run(media.download_message_media(client, doc_message(doc_id=99), tmp_path, 6))
    assert not cached and replaced.read_bytes() == b"NEWNEW" and client.calls == 2


def test_truncated_cached_file_is_redownloaded(tmp_path):
    client = FakeClient([[b"AAABBB"], [b"AAABBB"]])
    path, _ = asyncio.run(media.download_message_media(client, doc_message(), tmp_path, 6))
    Path(path).write_bytes(b"AAA")
    _, cached = asyncio.run(media.download_message_media(client, doc_message(), tmp_path, 6))
    assert not cached and Path(path).read_bytes() == b"AAABBB"
