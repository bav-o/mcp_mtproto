"""Чистый запуск: настоящий MCP initialize + tools/list через stdio (ревью, п. 1)."""

import asyncio
import os
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

ROOT = Path(__file__).resolve().parent.parent

EXPECTED_TOOLS = {
    "list_dialogs", "join_channel", "get_history", "search_messages", "export_chat_md", "export_status",
    "export_resume", "export_cancel", "download_media", "list_participants", "send_to_bot",
    "get_recent_bot_messages", "click_button",
}


def test_stdio_initialize_and_list_tools():
    async def run():
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "app.server"],
            env={**os.environ, "PYTHONPATH": str(ROOT)},
            cwd=str(ROOT),
        )
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await asyncio.wait_for(session.initialize(), 30)
                tools = await asyncio.wait_for(session.list_tools(), 30)
                return {t.name: t for t in tools.tools}

    tools = asyncio.run(run())
    assert set(tools) == EXPECTED_TOOLS
    assert tools["get_history"].annotations.readOnlyHint is True
    assert tools["send_to_bot"].annotations.readOnlyHint is False


def test_requirements_pin_mcp_1x():
    reqs = (ROOT / "requirements.txt").read_text(encoding="utf-8")
    assert "mcp>=" in reqs and ",<2" in reqs.split("mcp>=")[1].splitlines()[0]
