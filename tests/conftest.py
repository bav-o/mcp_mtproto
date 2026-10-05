"""Тесты работают без Telegram: подставные клиенты и временная папка данных.

Переменные окружения выставляются до импорта app.*, потому что пути вычисляются при импорте.
"""

import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace as NS

os.environ["MTPROTO_DATA_DIR"] = tempfile.mkdtemp(prefix="mtproto-tests-")
os.environ["PYTHON_DOTENV_DISABLED"] = "1"
os.environ.setdefault("TELEGRAM_API_ID", "1")
os.environ.setdefault("TELEGRAM_API_HASH", "0" * 32)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest  # noqa: E402
from telethon.tl.types import User  # noqa: E402

DATE = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)
HUMAN = User(id=111, first_name="Synthetic human", bot=False, access_hash=123)
BOT = User(id=222, first_name="Synthetic bot", bot=True, access_hash=456)


def message(mid=1, text="alpha", date=DATE, **kw):
    fields = dict(
        id=mid, date=date, sender=HUMAN, sender_id=HUMAN.id, text=text, message=text, media=None,
        fwd_from=None, reply_to_msg_id=None, buttons=None,
    )
    fields.update(kw)
    return NS(**fields)


@pytest.fixture
def data_dir():
    from app.client import DATA_DIR

    return DATA_DIR
