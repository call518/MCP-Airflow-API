"""Regression checks for trigger_dag request bodies per API version."""

from unittest.mock import AsyncMock, Mock

import pytest
from fastmcp import Client

from mcp_airflow_api.mcp_main import create_mcp_server
from mcp_airflow_api.tools import common_tools


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("version", "body", "date_key"),
    [
        ("v1", {"conf": {}}, "execution_date"),
        ("v2", {"conf": {}, "logical_date": None}, "logical_date"),
    ],
)
async def test_trigger_dag_body_and_date(monkeypatch, version, body, date_key):
    monkeypatch.setenv("AIRFLOW_API_VERSION", version)
    server = create_mcp_server()
    response = Mock()
    response.json.return_value = {
        "run_id": "manual__1",
        "state": "queued",
        date_key: "2026-11-12T00:00:00+00:00",
    }
    mocked = AsyncMock(return_value=response)
    monkeypatch.setattr(common_tools, "airflow_request", mocked)
    async with Client(server) as client:
        result = await client.call_tool("trigger_dag", {"dag_id": "asset1_producer"})
    assert mocked.call_args.args == ("POST", "/dags/asset1_producer/dagRuns")
    assert mocked.call_args.kwargs == {"json": body}
    assert result.data["run_id"] == "manual__1"
    assert result.data["execution_date"] == "2026-11-12T00:00:00+00:00"
