"""Regression checks for response fields that differ between API v1 and v2."""

from unittest.mock import AsyncMock, Mock

import pytest
from fastmcp import Client

from mcp_airflow_api import functions
from mcp_airflow_api.mcp_main import create_mcp_server
from mcp_airflow_api.tools import common_tools

DATE_FIELD = {"v1": "execution_date", "v2": "logical_date"}
RUN_DATE = "2026-11-12T00:00:00+00:00"


def dag_run(version, run_type="manual"):
    run = {
        "dag_id": "etl", "dag_run_id": "manual__1", "run_type": run_type,
        "state": "success", DATE_FIELD[version]: RUN_DATE,
        "start_date": "2026-11-12T00:00:00+00:00",
        "end_date": "2026-11-12T00:00:30+00:00",
    }
    if version == "v1":
        run["external_trigger"] = run_type == "manual"
    return run


def dag_details(version):
    if version == "v1":
        return {
            "dag_id": "etl", "is_active": True, "next_dagrun": RUN_DATE,
            "schedule_interval": {"__type": "CronExpression", "value": "@daily"},
            "start_date": "2026-01-01T00:00:00+00:00", "catchup": False,
        }
    return {
        "dag_id": "etl", "is_stale": False, "next_dagrun_logical_date": RUN_DATE,
        "timetable_summary": "@daily",
        "start_date": "2026-01-01T00:00:00+00:00", "catchup": False,
    }


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def airflow(monkeypatch, request):
    """Start a server for one API version with routed Airflow responses."""
    version = request.param
    monkeypatch.setenv("AIRFLOW_API_VERSION", version)
    server = create_mcp_server()
    routes = {}

    async def respond(method, url, **kwargs):
        path = url.split("?")[0]
        response = Mock()
        response.json.return_value = routes[path]
        return response

    mocked = AsyncMock(side_effect=respond)
    monkeypatch.setattr(common_tools, "airflow_request", mocked)
    monkeypatch.setattr(functions, "airflow_request", mocked)
    async with Client(server) as client:
        yield version, client, routes, mocked


VERSIONS = pytest.mark.parametrize("airflow", ["v1", "v2"], indirect=True)


@pytest.mark.anyio
@VERSIONS
async def test_running_dags_maps_run_fields(airflow):
    version, client, routes, _ = airflow
    routes["/dags/~/dagRuns"] = {"dag_runs": [dag_run(version)]}
    result = await client.call_tool("running_dags", {})
    run = result.data["dag_runs"][0]
    assert run["run_id"] == "manual__1"
    assert run["execution_date"] == RUN_DATE
    assert run["external_trigger"] is True


@pytest.mark.anyio
@VERSIONS
async def test_latest_run_lookup_orders_by_version_date(airflow):
    version, client, routes, mocked = airflow
    routes["/dags/etl/dagRuns"] = {"dag_runs": [dag_run(version)]}
    routes["/dags/etl/dagRuns/manual__1/taskInstances"] = {"task_instances": []}
    result = await client.call_tool("dag_task_duration", {"dag_id": "etl"})
    assert result.data["dag_run_id"] == "manual__1"
    first_url = mocked.call_args_list[0].args[1]
    assert f"order_by=-{DATE_FIELD[version]}" in first_url


@pytest.mark.anyio
@VERSIONS
async def test_get_dag_uses_details_and_version_fields(airflow):
    version, client, routes, mocked = airflow
    routes["/dags/etl/details"] = dag_details(version)
    result = await client.call_tool("get_dag", {"dag_id": "etl"})
    assert mocked.call_args.args == ("GET", "/dags/etl/details")
    assert result.data["is_active"] is True
    assert result.data["next_dagrun"] == RUN_DATE
    assert result.data["start_date"] == "2026-01-01T00:00:00+00:00"
    assert result.data["catchup"] is False
    expected_schedule = dag_details(version).get(
        "schedule_interval", "@daily"
    )
    assert result.data["schedule_interval"] == expected_schedule


@pytest.mark.anyio
@VERSIONS
async def test_dag_graph_derives_upstream_ids(airflow):
    _, client, routes, _ = airflow
    routes["/dags/etl/tasks"] = {"tasks": [
        {"task_id": "extract", "downstream_task_ids": ["transform"]},
        {"task_id": "transform", "downstream_task_ids": ["load"]},
        {"task_id": "load", "downstream_task_ids": []},
    ]}
    result = await client.call_tool("dag_graph", {"dag_id": "etl"})
    graph = result.data["task_graph"]
    assert graph["extract"]["upstream_task_ids"] == []
    assert graph["transform"]["upstream_task_ids"] == ["extract"]
    assert graph["load"]["upstream_task_ids"] == ["transform"]


@pytest.mark.anyio
@VERSIONS
async def test_import_summary_reads_error_fields(airflow):
    _, client, routes, _ = airflow
    routes["/importErrors"] = {"import_errors": [{
        "import_error_id": 7, "filename": "etl.py",
        "timestamp": RUN_DATE, "stack_trace": "Traceback",
    }]}
    result = await client.call_tool("all_dag_import_summary", {})
    error = result.data["import_errors_summary"]["etl.py"][0]
    assert error["id"] == 7
    assert error["stacktrace"].startswith("Traceback")
