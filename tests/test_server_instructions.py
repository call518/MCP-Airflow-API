"""Verify the MCP server instructions sent to clients."""

import pytest
from fastmcp import Client

from mcp_airflow_api.mcp_main import SERVER_INSTRUCTIONS_PATH, create_mcp_server

# Claude Code truncates server instructions beyond this many characters.
MAX_INSTRUCTIONS_CHARS = 4096


@pytest.fixture
def anyio_backend() -> str:
    """Use the server's asyncio backend."""
    return "asyncio"


def test_instructions_file_is_within_limit() -> None:
    """The instructions file is non-empty and fits the client limit."""
    with open(SERVER_INSTRUCTIONS_PATH, encoding="utf-8") as f:
        instructions = f.read()

    assert instructions.strip()
    assert len(instructions) <= MAX_INSTRUCTIONS_CHARS


@pytest.mark.anyio
@pytest.mark.parametrize("api_version", ["v1", "v2"])
async def test_instructions_reach_client_without_prompts(
    monkeypatch: pytest.MonkeyPatch, api_version: str
) -> None:
    """Clients receive the instructions at initialization and no MCP prompts."""
    monkeypatch.setenv("AIRFLOW_API_VERSION", api_version)
    with open(SERVER_INSTRUCTIONS_PATH, encoding="utf-8") as f:
        expected = f.read()

    async with Client(create_mcp_server()) as client:
        # FastMCP 3.4 has no discovery-mode Client; instructions arrive via
        # the initialize response.
        assert client.initialize_result is not None
        assert client.initialize_result.instructions == expected
        assert await client.list_prompts() == []
