"""Verify documentation exposed through the MCP protocol in both API versions."""

import re
from pathlib import Path

import pytest
from fastmcp import Client

from mcp_airflow_api.mcp_main import create_mcp_server


@pytest.fixture
def anyio_backend() -> str:
    """Use the server's asyncio backend."""
    return "asyncio"


@pytest.mark.anyio
@pytest.mark.parametrize("api_version, expected_count", [("v1", 54), ("v2", 56)])
async def test_tool_documentation(
    monkeypatch: pytest.MonkeyPatch, api_version: str, expected_count: int
) -> None:
    """Every registered tool and parameter has client-visible documentation."""
    monkeypatch.setenv("AIRFLOW_API_VERSION", api_version)
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text()

    async with Client(create_mcp_server()) as client:
        tools = await client.list_tools()
        assert len(tools) == expected_count

        for tool in tools:
            assert re.search(rf"\b{re.escape(tool.name)}\b", readme), tool.name
            for parameter, schema in tool.inputSchema.get("properties", {}).items():
                assert schema.get("description", "").strip(), (
                    f"{tool.name}.{parameter} has no description"
                )

        prompt = await client.get_prompt("airflow_cluster_monitoring")
        content = prompt.messages[0].content
        assert content.type == "text"
        assert f"{expected_count} " in content.text
        if api_version == "v2":
            assert "8 common tools are v1-only" in content.text
