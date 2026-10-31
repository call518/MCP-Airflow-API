"""Regression checks for task-instance listing routes and filters (issue #76)."""

from unittest.mock import AsyncMock, Mock
from urllib.parse import parse_qs, urlsplit

import pytest
from fastmcp import Client

from mcp_airflow_api.mcp_main import create_mcp_server
from mcp_airflow_api.tools import common_tools


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def listing_client(monkeypatch, request):
    monkeypatch.setenv("AIRFLOW_API_VERSION", request.param)
    server = create_mcp_server()
    response = Mock()
    response.json.return_value = {
        "task_instances": [{"state": "failed"}, {"state": "running"}],
        "total_entries": 20,
    }
    mocked = AsyncMock(return_value=response)
    monkeypatch.setattr(common_tools, "airflow_request", mocked)
    async with Client(server) as client:
        yield client, mocked


@pytest.mark.anyio
@pytest.mark.parametrize("listing_client", ["v1", "v2"], indirect=True)
@pytest.mark.parametrize("tool", ["list_task_instances_all", "list_task_instances_batch"])
async def test_wildcard_lists_and_repeated_filters(listing_client, tool):
    client, mocked = listing_client
    result = await client.call_tool(tool, {
        "state": ["failed", "running"],
        "start_date_gte": "2026-10-30T00:00:00+09:00",
        "limit": 2, "offset": 3,
    })
    method, url = mocked.call_args.args
    assert method == "GET"
    assert urlsplit(url).path == "/dags/~/dagRuns/~/taskInstances"
    assert parse_qs(urlsplit(url).query) == {
        "state": ["failed", "running"],
        "start_date_gte": ["2026-10-30T00:00:00+09:00"],
        "limit": ["2"], "offset": ["3"],
    }
    assert result.data["total_entries"] == 20
    if tool.endswith("batch"):
        assert result.data["state_summary"] == {"failed": 1, "running": 1}


@pytest.mark.anyio
@pytest.mark.parametrize("listing_client", ["v1", "v2"], indirect=True)
async def test_dates_zero_duration_and_path_encoding(listing_client):
    client, mocked = listing_client
    await client.call_tool("list_task_instances_all", {
        "dag_id": "demo", "dag_run_id": "manual__2026-10-30T00:00:00+09:00",
        "execution_date_gte": "2026-10-01T00:00:00+09:00",
        "execution_date_lte": "2026-11-01T00:00:00+09:00",
        "duration_gte": 0, "duration_lte": 0,
        "pool": ["pool a", "pool&b"],
    })
    method, url = mocked.call_args.args
    assert method == "GET"
    assert urlsplit(url).path == (
        "/dags/demo/dagRuns/manual__2026-10-30T00%3A00%3A00%2B09%3A00/taskInstances"
    )
    query = parse_qs(urlsplit(url).query)
    prefix = "logical_date" if common_tools.get_api_version() == "v2" else "execution_date"
    assert query[f"{prefix}_gte"] == ["2026-10-01T00:00:00+09:00"]
    assert query[f"{prefix}_lte"] == ["2026-11-01T00:00:00+09:00"]
    assert query["duration_gte"] == query["duration_lte"] == ["0.0"]
    assert query["pool"] == ["pool a", "pool&b"]
    assert "dag_id" not in query and "dag_run_id" not in query


@pytest.mark.anyio
@pytest.mark.parametrize("listing_client", ["v1"], indirect=True)
async def test_v1_task_filter_uses_batch_before_pagination(listing_client):
    client, mocked = listing_client
    await client.call_tool("list_task_instances_all", {
        "dag_id": "demo", "dag_run_id": "run", "task_id": "task",
        "state": ["failed", "running"], "duration_lte": 0,
        "execution_date_gte": "2026-10-01T00:00:00Z", "limit": 2, "offset": 3,
    })
    mocked.assert_awaited_once_with("POST", "/dags/~/dagRuns/~/taskInstances/list", json={
        "page_limit": 2, "page_offset": 3,
        "dag_ids": ["demo"], "dag_run_ids": ["run"], "task_ids": ["task"],
        "state": ["failed", "running"], "duration_lte": 0.0,
        "execution_date_gte": "2026-10-01T00:00:00Z",
    })


@pytest.mark.anyio
@pytest.mark.parametrize("listing_client", ["v2"], indirect=True)
@pytest.mark.parametrize("dag_id", [None, "~"])
async def test_v2_task_filter_and_run_without_dag(listing_client, dag_id):
    client, mocked = listing_client
    await client.call_tool("list_task_instances_all", {"task_id": "task"})
    assert parse_qs(urlsplit(mocked.call_args.args[1]).query)["task_id"] == ["task"]
    await client.call_tool("list_task_instances_all", {
        "dag_id": dag_id, "dag_run_id": "run", "task_id": "task",
        "limit": 2, "offset": 3,
        "execution_date_lte": "2026-11-01T00:00:00Z",
    })
    mocked.assert_awaited_with("POST", "/dags/~/dagRuns/~/taskInstances/list", json={
        "page_limit": 2, "page_offset": 3,
        "dag_run_ids": ["run"], "task_ids": ["task"],
        "logical_date_lte": "2026-11-01T00:00:00Z",
    })
