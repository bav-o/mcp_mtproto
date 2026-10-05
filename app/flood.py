"""FloodWait retry utility.

Vendored from TGgrabber (Magerko/TGgrabber, MIT License).
Origin: app/telethon_client/listener.py — retry_on_flood function.
Commit: 94ec1ba4a3202ac85cfc1c952350ada85ab3ac84
Adaptations: typed annotations, % log format, named fallback constant.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from telethon.errors import FloodWaitError

log = logging.getLogger(__name__)

_FLOOD_WAIT_FALLBACK_SECS: int = 60
_FLOOD_CRITICAL_SECS: int = 300  # > 5 min → critical log


async def retry_on_flood(
    func: Callable[..., Any],
    *args: Any,
    max_retries: int = 3,
    **kwargs: Any,
) -> Any:
    """Call func; on FloodWaitError wait and retry up to max_retries times.

    Logs warning for waits ≤ 300s; critical for waits > 300s.
    On exhaustion re-raises — caller handles.
    """
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except FloodWaitError as exc:
            wait = exc.seconds if hasattr(exc, "seconds") else _FLOOD_WAIT_FALLBACK_SECS
            if attempt < max_retries - 1:
                if wait > _FLOOD_CRITICAL_SECS:
                    log.critical(
                        "telethon: FloodWait %ds (>%ds) — retrying (%d/%d)",
                        wait,
                        _FLOOD_CRITICAL_SECS,
                        attempt + 1,
                        max_retries,
                        extra={"category": "telethon"},
                    )
                else:
                    log.warning(
                        "telethon: FloodWait %ds — retrying (%d/%d)",
                        wait,
                        attempt + 1,
                        max_retries,
                        extra={"category": "telethon"},
                    )
                await asyncio.sleep(wait)
            else:
                log.error(
                    "telethon: FloodWait — max retries (%d) exceeded, re-raising",
                    max_retries,
                    extra={"category": "telethon"},
                )
                raise
