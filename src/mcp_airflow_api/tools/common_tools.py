"""
Common tools shared between v1 and v2 APIs.
Registers 54 tools in both API versions.
User, permission, role, and dataset operations are supported only in v1.
"""

from typing import Any, Dict, List, Optional
from ..functions import (
    read_prompt_template, parse_prompt_sections, 
    get_current_time_context, list_dags_internal, get_dag_detailed_info, 
    PROMPT_TEMPLATE_PATH
)
import logging

logger = logging.getLogger(__name__)

# Global variable to hold the version-specific airflow_request function
# This will be set by v1_tools.py or v2_tools.py during registration
airflow_request = None

def register_common_tools(mcp):
    """Register 54 common tools, including operations supported only in v1."""
    
    if airflow_request is None:
        raise RuntimeError("airflow_request function must be set before registering common tools")
    
    logger.info("Registering common tools shared between v1 and v2")

    # Template & Prompt Management
    @mcp.tool()
    async def get_prompt_template(section: Optional[str] = None, mode: Optional[str] = None) -> str:
        """
        [Tool Role]: Provides comprehensive prompt template for LLM interactions with Airflow operations.

        Args:
            section: Optional section name to get specific part of template
            mode: Optional mode (summary/detailed) to control response verbosity

        Returns:
            Comprehensive template or specific section for optimal LLM guidance
        """
        template = read_prompt_template(PROMPT_TEMPLATE_PATH)
        
        if section:
            sections = parse_prompt_sections(template)
            for i, s in enumerate(sections):
                if section.lower() in s.lower():
                    return sections[i + 1]  # +1 to skip the title section
            return f"Section '{section}' not found."
    
        return template

    # DAG Management (11 tools)
    @mcp.tool()
    async def list_dags(limit: int = 20,
                  offset: int = 0,
                  fetch_all: bool = False,
                  id_contains: Optional[str] = None,
                  name_contains: Optional[str] = None) -> Dict[str, Any]:
        """
        [Tool Role]: Lists all DAGs registered in the Airflow cluster with pagination support.
    
        Args:
            limit: Maximum number of DAGs to return (default: 20)
            offset: Number of DAGs to skip for pagination (default: 0)
            fetch_all: If True, fetches all DAGs regardless of limit/offset
            id_contains: Filter DAGs by ID containing this string
            name_contains: Filter DAGs by display name containing this string

        Returns:
            Dict containing dags list, pagination info, and total counts
        """
        return await list_dags_internal(limit, offset, fetch_all, id_contains, name_contains)

    @mcp.tool()
    async def get_dag(dag_id: str) -> Dict[str, Any]:
        """
        [Tool Role]: Retrieves detailed information for a specific DAG.

        Args:
            dag_id: The DAG ID to get details for

        Returns:
            Comprehensive DAG details
        """
        return await get_dag_detailed_info(dag_id)

    @mcp.tool()
    async def get_dags_detailed_batch(
        limit: int = 100,
        offset: int = 0,
        fetch_all: bool = False,
        id_contains: Optional[str] = None,
        name_contains: Optional[str] = None,
        is_active: Optional[bool] = None,
        is_paused: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        [Tool Role]: Retrieves detailed information for multiple DAGs in batch with
        latest run information.

        Args:
            limit: Maximum number of records to request per page. Default:
                100.
            offset: Number of records to skip for pagination. Default: 0.
            fetch_all: Fetch every DAG page; ignores limit and offset when
                True. Default: False.
            id_contains: Optional substring to match against DAG
                identifiers. Default: None.
            name_contains: Optional substring to match against DAG display
                names. Default: None.
            is_active: Optional active-status filter; None includes both
                statuses. Default: None.
            is_paused: Optional paused-status filter; None includes both
                statuses. Default: None.
        """
        dag_list_result = await list_dags_internal(
            limit=limit, 
            offset=offset, 
            fetch_all=fetch_all,
            id_contains=id_contains,
            name_contains=name_contains
        )
    
        dags_basic = dag_list_result.get("dags", [])
        detailed_dags = []
        success_count = 0
        error_count = 0
        errors = []
        skipped_count = 0
    
        for dag_basic in dags_basic:
            dag_id = dag_basic.get("dag_id")
            if not dag_id:
                skipped_count += 1
                continue
            
            if is_active is not None and dag_basic.get("is_active") != is_active:
                skipped_count += 1
                continue
            if is_paused is not None and dag_basic.get("is_paused") != is_paused:
                skipped_count += 1
                continue
            
            try:
                detailed_dag = await get_dag_detailed_info(dag_id)
                
                try:
                    latest_run_resp = await airflow_request("GET", f"/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date")
                    latest_run_resp.raise_for_status()
                    latest_runs = latest_run_resp.json().get("dag_runs", [])
                
                    if latest_runs:
                        latest_run = latest_runs[0]
                        detailed_dag["latest_dag_run"] = {
                            "run_id": latest_run.get("run_id"),
                            "run_type": latest_run.get("run_type"),
                            "state": latest_run.get("state"),
                            "execution_date": latest_run.get("execution_date"),
                            "start_date": latest_run.get("start_date"),
                            "end_date": latest_run.get("end_date"),
                            "data_interval_start": latest_run.get("data_interval_start"),
                            "data_interval_end": latest_run.get("data_interval_end"),
                            "external_trigger": latest_run.get("external_trigger"),
                            "conf": latest_run.get("conf"),
                            "note": latest_run.get("note")
                        }
                    else:
                        detailed_dag["latest_dag_run"] = None
                except Exception:
                    detailed_dag["latest_dag_run"] = None
                
                detailed_dags.append(detailed_dag)
                success_count += 1
            except Exception as e:
                error_count += 1
                errors.append({
                    "dag_id": dag_id,
                    "error": str(e)
                })
    
        return {
            "dags_detailed": detailed_dags,
            "total_processed": success_count,
            "total_available": dag_list_result.get("total_entries", 0),
            "returned_count": len(detailed_dags),
            "processing_stats": {
                "success_count": success_count,
                "error_count": error_count,
                "skipped_count": skipped_count,
                "errors": errors
            },
            "applied_filters": {
                "id_contains": id_contains,
                "name_contains": name_contains,
                "is_active": is_active,
                "is_paused": is_paused,
                "limit": limit,
                "offset": offset,
                "fetch_all": fetch_all
            },
            "pagination_info": dag_list_result.get("pagination_info", {}),
            "has_more_pages": dag_list_result.get("has_more_pages", False),
            "next_offset": dag_list_result.get("next_offset")
        }

    @mcp.tool()
    async def running_dags() -> Dict[str, Any]:
        """[Tool Role]: Lists all currently running DAG runs in the Airflow cluster."""
        resp = await airflow_request("GET", "/dags/~/dagRuns?state=running&limit=1000&order_by=-start_date")
        resp.raise_for_status()
        data = resp.json()
    
        running_runs = []
        for run in data.get("dag_runs", []):
            run_info = {
                "dag_id": run.get("dag_id"),
                "dag_display_name": run.get("dag_display_name"),
                "run_id": run.get("run_id"),
                "run_type": run.get("run_type"),
                "state": run.get("state"),
                "execution_date": run.get("execution_date"),
                "start_date": run.get("start_date"),
                "end_date": run.get("end_date"),
                "data_interval_start": run.get("data_interval_start"),
                "data_interval_end": run.get("data_interval_end"),
                "external_trigger": run.get("external_trigger"),
                "conf": run.get("conf"),
                "note": run.get("note")
            }
            running_runs.append(run_info)
    
        return {
            "dag_runs": running_runs,
            "total_running": len(running_runs),
            "query_info": {
                "state_filter": "running",
                "limit": 1000,
                "order_by": "start_date (descending)"
            }
        }

    @mcp.tool()
    async def failed_dags() -> Dict[str, Any]:
        """[Tool Role]: Lists all recently failed DAG runs in the Airflow cluster."""
        resp = await airflow_request("GET", "/dags/~/dagRuns?state=failed&limit=1000&order_by=-start_date")
        resp.raise_for_status()
        data = resp.json()
    
        failed_runs = []
        for run in data.get("dag_runs", []):
            run_info = {
                "dag_id": run.get("dag_id"),
                "dag_display_name": run.get("dag_display_name"),
                "run_id": run.get("run_id"),
                "run_type": run.get("run_type"),
                "state": run.get("state"),
                "execution_date": run.get("execution_date"),
                "start_date": run.get("start_date"),
                "end_date": run.get("end_date"),
                "data_interval_start": run.get("data_interval_start"),
                "data_interval_end": run.get("data_interval_end"),
                "external_trigger": run.get("external_trigger"),
                "conf": run.get("conf"),
                "note": run.get("note")
            }
            failed_runs.append(run_info)
    
        return {
            "dag_runs": failed_runs,
            "total_failed": len(failed_runs),
            "query_info": {
                "state_filter": "failed",
                "limit": 1000,
                "order_by": "start_date (descending)"
            }
        }

    @mcp.tool()
    async def trigger_dag(dag_id: str) -> Dict[str, Any]:
        """
        [Tool Role]: Triggers a new DAG run for a specified Airflow DAG.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
        """
        if not dag_id:
            raise ValueError("dag_id must not be empty")
        resp = await airflow_request("POST", f"/dags/{dag_id}/dagRuns", json={"conf": {}})
        resp.raise_for_status()
        run = resp.json()
        return {
            "dag_id": dag_id,
            "run_id": run.get("run_id"),
            "state": run.get("state"),
            "execution_date": run.get("execution_date"),
            "start_date": run.get("start_date"),
            "end_date": run.get("end_date")
        }

    @mcp.tool()
    async def pause_dag(dag_id: str) -> Dict[str, Any]:
        """
        [Tool Role]: Pauses the specified Airflow DAG (prevents scheduling new runs).

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
        """
        if not dag_id:
            raise ValueError("dag_id must not be empty")
        resp = await airflow_request("PATCH", f"/dags/{dag_id}", json={"is_paused": True})
        resp.raise_for_status()
        dag_data = resp.json()
        return {
            "dag_id": dag_id,
            "is_paused": dag_data.get("is_paused")
        }

    @mcp.tool()
    async def unpause_dag(dag_id: str) -> Dict[str, Any]:
        """
        [Tool Role]: Unpauses the specified Airflow DAG (allows scheduling new runs).

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
        """
        if not dag_id:
            raise ValueError("dag_id must not be empty")
        resp = await airflow_request("PATCH", f"/dags/{dag_id}", json={"is_paused": False})
        resp.raise_for_status()
        dag_data = resp.json()
        return {
            "dag_id": dag_id,
            "is_paused": dag_data.get("is_paused")
        }

    @mcp.tool()
    async def dag_graph(dag_id: str) -> Dict[str, Any]:
        """
        [Tool Role]: Retrieves task graph structure for the specified DAG.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
        """
        if not dag_id:
            raise ValueError("dag_id must not be empty")
        resp = await airflow_request("GET", f"/dags/{dag_id}/tasks")
        resp.raise_for_status()
        tasks_data = resp.json()
        
        tasks = tasks_data.get("tasks", [])
        task_graph = {}
        
        for task in tasks:
            task_id = task.get("task_id")
            task_graph[task_id] = {
                "task_id": task_id,
                "task_type": task.get("class_ref", {}).get("class_name"),
                "downstream_task_ids": task.get("downstream_task_ids", []),
                "upstream_task_ids": task.get("upstream_task_ids", [])
            }
        
        return {
            "dag_id": dag_id,
            "task_graph": task_graph,
            "total_tasks": len(tasks),
            "task_relationships": {
                "nodes": list(task_graph.keys()),
                "edges": [(task_id, downstream) for task_id, task_info in task_graph.items() 
                         for downstream in task_info["downstream_task_ids"]]
            }
        }

    @mcp.tool()
    async def list_tasks(dag_id: str) -> Dict[str, Any]:
        """
        [Tool Role]: Lists all tasks within the specified DAG.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
        """
        if not dag_id:
            raise ValueError("dag_id must not be empty")
        resp = await airflow_request("GET", f"/dags/{dag_id}/tasks")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def dag_code(dag_id: str) -> Dict[str, Any]:
        """
        [Tool Role]: Retrieves the source code for the specified DAG.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
        """
        if not dag_id:
            raise ValueError("dag_id must not be empty")
        resp = await airflow_request("GET", f"/dagSources/{dag_id}")
        resp.raise_for_status()
        return resp.json()

    # Continue with remaining tools...
    
    # Event/Log Management (3 tools)
    @mcp.tool()
    async def list_event_logs(dag_id: Optional[str] = None, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """
        [Tool Role]: Lists event logs from Airflow.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name. Default: None.
            limit: Maximum number of records to request per page. Default:
                20.
            offset: Number of records to skip for pagination. Default: 0.
        """
        params = {'limit': limit, 'offset': offset}
        if dag_id:
            params['dag_id'] = dag_id
        
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        resp = await airflow_request("GET", f"/eventLogs?{query_string}")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_event_log(event_log_id: int) -> Dict[str, Any]:
        """
        [Tool Role]: Retrieves a specific event log entry.

        Args:
            event_log_id: Numeric identifier of the event log entry.
        """
        resp = await airflow_request("GET", f"/eventLogs/{event_log_id}")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def all_dag_event_summary() -> Dict[str, Any]:
        """[Tool Role]: Provides summary of event logs across all DAGs."""
        resp = await airflow_request("GET", "/eventLogs?limit=1000")
        resp.raise_for_status()
        data = resp.json()
        
        event_summary = {}
        for event in data.get("event_logs", []):
            dag_id = event.get("dag_id", "unknown")
            event_type = event.get("event", "unknown")
            
            if dag_id not in event_summary:
                event_summary[dag_id] = {}
            if event_type not in event_summary[dag_id]:
                event_summary[dag_id][event_type] = 0
            event_summary[dag_id][event_type] += 1
        
        return {
            "event_summary": event_summary,
            "total_events": len(data.get("event_logs", [])),
            "unique_dags": len(event_summary)
        }

    # Import Error Management (3 tools)
    @mcp.tool()
    async def list_import_errors(limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """
        [Tool Role]: Lists import errors in Airflow.

        Args:
            limit: Maximum number of records to request per page. Default:
                20.
            offset: Number of records to skip for pagination. Default: 0.
        """
        params = {'limit': limit, 'offset': offset}
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        resp = await airflow_request("GET", f"/importErrors?{query_string}")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_import_error(import_error_id: int) -> Dict[str, Any]:
        """
        [Tool Role]: Retrieves a specific import error.

        Args:
            import_error_id: Numeric identifier of the DAG import error.
        """
        resp = await airflow_request("GET", f"/importErrors/{import_error_id}")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def all_dag_import_summary() -> Dict[str, Any]:
        """[Tool Role]: Provides summary of import errors across all DAGs."""
        resp = await airflow_request("GET", "/importErrors?limit=1000")
        resp.raise_for_status()
        data = resp.json()
        
        import_summary = {}
        for error in data.get("import_errors", []):
            filename = error.get("filename", "unknown")
            if filename not in import_summary:
                import_summary[filename] = []
            import_summary[filename].append({
                "id": error.get("id"),
                "timestamp": error.get("timestamp"),
                "stacktrace": error.get("stacktrace", "")[:200] + "..."
            })
        
        return {
            "import_errors_summary": import_summary,
            "total_errors": len(data.get("import_errors", [])),
            "files_with_errors": len(import_summary)
        }

    # Analysis/Statistics (3 tools)
    @mcp.tool()
    async def dag_run_duration(dag_id: str, limit: int = 10) -> Dict[str, Any]:
        """
        [Tool Role]: Analyzes DAG run durations and performance metrics.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
            limit: Maximum number of recent DAG runs to analyze (default:
                10).
        """
        if not dag_id:
            raise ValueError("dag_id must not be empty")
        
        resp = await airflow_request("GET", f"/dags/{dag_id}/dagRuns?limit={limit}&order_by=-execution_date")
        resp.raise_for_status()
        data = resp.json()
        
        runs = data.get("dag_runs", [])
        durations = []
        
        for run in runs:
            start_date = run.get("start_date")
            end_date = run.get("end_date")
            if start_date and end_date:
                from datetime import datetime
                start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                duration_seconds = (end - start).total_seconds()
                durations.append({
                    "run_id": run.get("run_id"),
                    "duration_seconds": duration_seconds,
                    "state": run.get("state"),
                    "execution_date": run.get("execution_date")
                })
        
        avg_duration = sum(d["duration_seconds"] for d in durations) / len(durations) if durations else 0
        
        return {
            "dag_id": dag_id,
            "run_durations": durations,
            "statistics": {
                "average_duration_seconds": avg_duration,
                "total_analyzed_runs": len(durations),
                "fastest_run": min(durations, key=lambda x: x["duration_seconds"]) if durations else None,
                "slowest_run": max(durations, key=lambda x: x["duration_seconds"]) if durations else None
            }
        }

    @mcp.tool()
    async def dag_task_duration(dag_id: str, dag_run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        [Tool Role]: Analyzes task durations within a DAG run.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
            dag_run_id: DAG run identifier; omitted or empty selects the
                latest run.
        """
        if not dag_id:
            raise ValueError("dag_id must not be empty")
        
        if not dag_run_id:
            # Get the latest run
            resp = await airflow_request("GET", f"/dags/{dag_id}/dagRuns?limit=1&order_by=-execution_date")
            resp.raise_for_status()
            runs = resp.json().get("dag_runs", [])
            if not runs:
                return {"error": f"No DAG runs found for DAG {dag_id}"}
            dag_run_id = runs[0]["run_id"]
        
        resp = await airflow_request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")
        resp.raise_for_status()
        data = resp.json()
        
        task_durations = []
        for task in data.get("task_instances", []):
            start_date = task.get("start_date")
            end_date = task.get("end_date")
            if start_date and end_date:
                from datetime import datetime
                start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                duration_seconds = (end - start).total_seconds()
                task_durations.append({
                    "task_id": task.get("task_id"),
                    "duration_seconds": duration_seconds,
                    "state": task.get("state"),
                    "start_date": start_date,
                    "end_date": end_date
                })
        
        return {
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "task_durations": task_durations,
            "total_tasks": len(task_durations)
        }

    @mcp.tool()
    async def dag_calendar(dag_id: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        [Tool Role]: Shows DAG schedule and execution calendar for a date range.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
            start_date: Inclusive run start-time lower bound in ISO 8601
                format.
            end_date: Inclusive run start-time upper bound in ISO 8601
                format.
        """
        if not dag_id:
            raise ValueError("dag_id must not be empty")
        
        params = {
            'start_date_gte': start_date,
            'start_date_lte': end_date,
            'limit': 1000
        }
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        
        resp = await airflow_request("GET", f"/dags/{dag_id}/dagRuns?{query_string}")
        resp.raise_for_status()
        data = resp.json()
        
        calendar_data = []
        for run in data.get("dag_runs", []):
            calendar_data.append({
                "execution_date": run.get("execution_date"),
                "start_date": run.get("start_date"),
                "end_date": run.get("end_date"),
                "state": run.get("state"),
                "run_type": run.get("run_type")
            })
        
        return {
            "dag_id": dag_id,
            "date_range": {"start": start_date, "end": end_date},
            "calendar_entries": calendar_data,
            "total_runs": len(calendar_data)
        }

    # System Information (6 tools)
    @mcp.tool()
    async def get_health() -> Dict[str, Any]:
        """[Tool Role]: Checks Airflow cluster health status."""
        # Import here to avoid circular imports
        from ..functions import get_api_version
        
        api_version = get_api_version()
        
        if api_version == "v2":
            # v2 API: Use /monitor/health endpoint (Airflow 3.x)
            resp = await airflow_request("GET", "/monitor/health")
        else:
            # v1 API: Use /health endpoint (Airflow 2.x)
            resp = await airflow_request("GET", "/health")
        
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_version() -> Dict[str, Any]:
        """[Tool Role]: Gets Airflow version information."""
        resp = await airflow_request("GET", "/version")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_config() -> Dict[str, Any]:
        """[Tool Role]: Retrieves Airflow configuration."""
        resp = await airflow_request("GET", "/config")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def list_config_sections() -> Dict[str, Any]:
        """[Tool Role]: Lists all configuration sections with summary."""
        try:
            resp = await airflow_request("GET", "/config")
            resp.raise_for_status()
            config_data = resp.json()
            
            sections_summary = {}
            for section_name, section_data in config_data.get("sections", {}).items():
                options_count = len(section_data.get("options", {}))
                sections_summary[section_name] = {
                    "options_count": options_count,
                    "sample_options": list(section_data.get("options", {}).keys())[:5]
                }
            
            return {
                "sections_summary": sections_summary,
                "total_sections": len(sections_summary)
            }
        except Exception as e:
            return {
                "error": f"Configuration access denied: {str(e)}",
                "note": "This requires 'expose_config = True' in airflow.cfg [webserver] section"
            }

    @mcp.tool()
    async def get_config_section(section_name: str) -> Dict[str, Any]:
        """
        [Tool Role]: Gets all options within a specific configuration section.

        Args:
            section_name: Exact Airflow configuration section name, such as
                core.
        """
        try:
            resp = await airflow_request("GET", "/config")
            resp.raise_for_status()
            config_data = resp.json()
            
            section_data = config_data.get("sections", {}).get(section_name)
            if not section_data:
                return {"error": f"Section '{section_name}' not found"}
            
            return {
                "section_name": section_name,
                "options": section_data.get("options", {}),
                "options_count": len(section_data.get("options", {}))
            }
        except Exception as e:
            return {
                "error": f"Configuration access denied: {str(e)}",
                "note": "This requires 'expose_config = True' in airflow.cfg [webserver] section"
            }

    @mcp.tool()
    async def search_config_options(search_term: str) -> Dict[str, Any]:
        """
        [Tool Role]: Searches for configuration options matching a term.

        Args:
            search_term: Case-insensitive substring to find in option names
                or values.
        """
        try:
            resp = await airflow_request("GET", "/config")
            resp.raise_for_status()
            config_data = resp.json()
            
            matching_options = {}
            for section_name, section_data in config_data.get("sections", {}).items():
                section_matches = {}
                for option_name, option_data in section_data.get("options", {}).items():
                    if search_term.lower() in option_name.lower() or search_term.lower() in str(option_data.get("value", "")).lower():
                        section_matches[option_name] = option_data
                
                if section_matches:
                    matching_options[section_name] = section_matches
            
            return {
                "search_term": search_term,
                "matching_options": matching_options,
                "total_matches": sum(len(section) for section in matching_options.values())
            }
        except Exception as e:
            return {
                "error": f"Configuration access denied: {str(e)}",
                "note": "This requires 'expose_config = True' in airflow.cfg [webserver] section"
            }

    # Pool Management (2 tools)
    @mcp.tool()
    async def list_pools(limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """
        [Tool Role]: Lists all pools in Airflow.

        Args:
            limit: Maximum number of records to request per page. Default:
                20.
            offset: Number of records to skip for pagination. Default: 0.
        """
        params = {'limit': limit, 'offset': offset}
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        resp = await airflow_request("GET", f"/pools?{query_string}")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_pool(pool_name: str) -> Dict[str, Any]:
        """
        [Tool Role]: Gets details for a specific pool.

        Args:
            pool_name: Exact Airflow pool name, such as default_pool.
        """
        resp = await airflow_request("GET", f"/pools/{pool_name}")
        resp.raise_for_status()
        return resp.json()

    # Task Instance Management (5 tools)
    @mcp.tool()
    async def list_task_instances_all(
        dag_id: Optional[str] = None,
        dag_run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        execution_date_gte: Optional[str] = None,
        execution_date_lte: Optional[str] = None,
        start_date_gte: Optional[str] = None,
        start_date_lte: Optional[str] = None,
        end_date_gte: Optional[str] = None,
        end_date_lte: Optional[str] = None,
        duration_gte: Optional[float] = None,
        duration_lte: Optional[float] = None,
        state: Optional[List[str]] = None,
        pool: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        [Tool Role]: Lists task instances with comprehensive filtering options.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name. Default: None.
            dag_run_id: Exact DAG run identifier for the selected DAG.
                Default: None.
            task_id: Exact task identifier within the selected DAG. Default:
                None.
            execution_date_gte: Optional inclusive task execution time lower
                bound, in ISO 8601 format. Default: None.
            execution_date_lte: Optional inclusive task execution time upper
                bound, in ISO 8601 format. Default: None.
            start_date_gte: Optional inclusive task start time lower bound,
                in ISO 8601 format. Default: None.
            start_date_lte: Optional inclusive task start time upper bound,
                in ISO 8601 format. Default: None.
            end_date_gte: Optional inclusive task end time lower bound, in
                ISO 8601 format. Default: None.
            end_date_lte: Optional inclusive task end time upper bound, in
                ISO 8601 format. Default: None.
            duration_gte: Optional minimum task duration in seconds.
                Default: None.
            duration_lte: Optional maximum task duration in seconds.
                Default: None.
            state: Optional list of task states, such as ["failed",
                "running"]. Default: None.
            pool: Optional list of Airflow pool names to filter task
                instances. Default: None.
            limit: Maximum number of records to request per page. Default:
                100.
            offset: Number of records to skip for pagination. Default: 0.
        """
        params = {'limit': limit, 'offset': offset}
        
        # Add all filter parameters
        if dag_id:
            params['dag_id'] = dag_id
        if dag_run_id:
            params['dag_run_id'] = dag_run_id
        if task_id:
            params['task_id'] = task_id
        if execution_date_gte:
            params['execution_date_gte'] = execution_date_gte
        if execution_date_lte:
            params['execution_date_lte'] = execution_date_lte
        if start_date_gte:
            params['start_date_gte'] = start_date_gte
        if start_date_lte:
            params['start_date_lte'] = start_date_lte
        if end_date_gte:
            params['end_date_gte'] = end_date_gte
        if end_date_lte:
            params['end_date_lte'] = end_date_lte
        if duration_gte:
            params['duration_gte'] = duration_gte
        if duration_lte:
            params['duration_lte'] = duration_lte
        if state:
            params['state'] = state
        if pool:
            params['pool'] = pool
        
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        resp = await airflow_request("GET", f"/taskInstances?{query_string}")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_task_instance_details(dag_id: str, dag_run_id: str, task_id: str) -> Dict[str, Any]:
        """
        [Tool Role]: Gets detailed information for a specific task instance.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
            dag_run_id: Exact DAG run identifier for the selected DAG.
            task_id: Exact task identifier within the selected DAG.
        """
        resp = await airflow_request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def list_task_instances_batch(
        limit: int = 100,
        offset: int = 0,
        start_date_gte: Optional[str] = None,
        start_date_lte: Optional[str] = None,
        state: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        [Tool Role]: Lists task instances in batch with date and state filtering.

        Args:
            limit: Maximum number of records to request per page. Default:
                100.
            offset: Number of records to skip for pagination. Default: 0.
            start_date_gte: Optional inclusive task start time lower bound,
                in ISO 8601 format. Default: None.
            start_date_lte: Optional inclusive task start time upper bound,
                in ISO 8601 format. Default: None.
            state: Optional list of task states, such as ["failed",
                "running"]. Default: None.
        """
        params = {'limit': limit, 'offset': offset}
        
        if start_date_gte:
            params['start_date_gte'] = start_date_gte
        if start_date_lte:
            params['start_date_lte'] = start_date_lte
        if state:
            params['state'] = state
        
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        resp = await airflow_request("GET", f"/taskInstances?{query_string}")
        resp.raise_for_status()
        data = resp.json()
        
        # Add summary statistics
        task_instances = data.get("task_instances", [])
        state_summary = {}
        for task in task_instances:
            task_state = task.get("state", "unknown")
            state_summary[task_state] = state_summary.get(task_state, 0) + 1
        
        data["state_summary"] = state_summary
        return data

    @mcp.tool()
    async def get_task_instance_extra_links(dag_id: str, dag_run_id: str, task_id: str) -> Dict[str, Any]:
        """
        [Tool Role]: Gets extra links for a task instance.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
            dag_run_id: Exact DAG run identifier for the selected DAG.
            task_id: Exact task identifier within the selected DAG.
        """
        resp = await airflow_request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/links")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_task_instance_logs(dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1) -> Dict[str, Any]:
        """
        [Tool Role]: Retrieves logs for a specific task instance.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
            dag_run_id: Exact DAG run identifier for the selected DAG.
            task_id: Exact task identifier within the selected DAG.
            try_number: Task attempt number whose logs to retrieve (default:
                1). Default: 1.
        """
        resp = await airflow_request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}")
        resp.raise_for_status()
        return resp.json()

    # Variable Management (2 tools)
    @mcp.tool()
    async def list_variables(limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """
        [Tool Role]: Lists all variables in Airflow.

        Args:
            limit: Maximum number of records to request per page. Default:
                20.
            offset: Number of records to skip for pagination. Default: 0.
        """
        params = {'limit': limit, 'offset': offset}
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        resp = await airflow_request("GET", f"/variables?{query_string}")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_variable(variable_key: str) -> Dict[str, Any]:
        """
        [Tool Role]: Gets the value of a specific variable.

        Args:
            variable_key: Exact key of the Airflow variable.
        """
        resp = await airflow_request("GET", f"/variables/{variable_key}")
        resp.raise_for_status()
        return resp.json()

    # XCom Management (2 tools)
    @mcp.tool()
    async def list_xcom_entries(dag_id: str, dag_run_id: str, task_id: str, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """
        [Tool Role]: Lists XCom entries for a specific task instance.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
            dag_run_id: Exact DAG run identifier for the selected DAG.
            task_id: Exact task identifier within the selected DAG.
            limit: Maximum number of records to request per page. Default:
                20.
            offset: Number of records to skip for pagination. Default: 0.
        """
        params = {'limit': limit, 'offset': offset}
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        resp = await airflow_request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries?{query_string}")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_xcom_entry(dag_id: str, dag_run_id: str, task_id: str, xcom_key: str) -> Dict[str, Any]:
        """
        [Tool Role]: Gets a specific XCom entry.

        Args:
            dag_id: Exact DAG identifier, as returned by list_dags; not its
                display name.
            dag_run_id: Exact DAG run identifier for the selected DAG.
            task_id: Exact task identifier within the selected DAG.
            xcom_key: Exact XCom key for the selected task instance.
        """
        resp = await airflow_request("GET", f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}")
        resp.raise_for_status()
        return resp.json()

    # Connection Management (5 tools)
    @mcp.tool()
    async def list_connections(limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """
        [Tool Role]: Lists all connections in Airflow.

        Args:
            limit: Maximum number of records to request per page. Default:
                20.
            offset: Number of records to skip for pagination. Default: 0.
        """
        params = {'limit': limit, 'offset': offset}
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        resp = await airflow_request("GET", f"/connections?{query_string}")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_connection(connection_id: str) -> Dict[str, Any]:
        """
        [Tool Role]: Gets details for a specific connection.

        Args:
            connection_id: Exact Airflow connection identifier.
        """
        resp = await airflow_request("GET", f"/connections/{connection_id}")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def create_connection(connection_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        [Tool Role]: Creates a new connection.

        Args:
            connection_data: Connection JSON body with connection_id and
                conn_type; optional host, login, password, schema, port, and
                extra fields. Obtain credentials from a secret store; do not
                hardcode them.
        """
        resp = await airflow_request("POST", "/connections", json=connection_data)
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def update_connection(connection_id: str, connection_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        [Tool Role]: Updates an existing connection.

        Args:
            connection_id: Exact Airflow connection identifier.
            connection_data: Connection JSON body containing fields to
                update, such as host, login, password, schema, port, or
                extra. Obtain credentials from a secret store; do not
                hardcode them.
        """
        resp = await airflow_request("PATCH", f"/connections/{connection_id}", json=connection_data)
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def delete_connection(connection_id: str) -> Dict[str, Any]:
        """
        [Tool Role]: Deletes a connection.

        Args:
            connection_id: Exact Airflow connection identifier.
        """
        resp = await airflow_request("DELETE", f"/connections/{connection_id}")
        resp.raise_for_status()
        return {"message": f"Connection {connection_id} deleted successfully"}

    # User & Permissions Management (4 tools) - v1 API only
    @mcp.tool()
    async def list_users(limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """
        [Tool Role]: Lists all users in the Airflow system (v1 API only).

        Args:
            limit: Maximum number of records to request per page. Default:
                20.
            offset: Number of records to skip for pagination. Default: 0.
        """
        from ..functions import get_api_version
        
        api_version = get_api_version()
        if api_version == "v2":
            return {"error": "User management is not available in Airflow 3.x (API v2)", "available_in": "v1 only"}
        
        params = []
        params.append(f"limit={limit}")
        if offset > 0:
            params.append(f"offset={offset}")
        
        query_string = "&".join(params) if params else ""
        endpoint = f"/users?{query_string}" if query_string else "/users"
        
        resp = await airflow_request("GET", endpoint)
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_user(username: str) -> Dict[str, Any]:
        """
        [Tool Role]: Gets details of a specific user (v1 API only).

        Args:
            username: Exact Airflow username (API v1 only).
        """
        from ..functions import get_api_version
        
        api_version = get_api_version()
        if api_version == "v2":
            return {"error": "User management is not available in Airflow 3.x (API v2)", "available_in": "v1 only"}
        
        resp = await airflow_request("GET", f"/users/{username}")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def list_permissions() -> Dict[str, Any]:
        """[Tool Role]: Lists all permissions available in the Airflow system (v1 API only)."""
        from ..functions import get_api_version
        
        api_version = get_api_version()
        if api_version == "v2":
            return {"error": "Permission management is not available in Airflow 3.x (API v2)", "available_in": "v1 only"}
        
        resp = await airflow_request("GET", "/permissions")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def list_roles(limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """
        [Tool Role]: Lists all roles in the Airflow system (v1 API only).

        Args:
            limit: Maximum number of records to request per page. Default:
                20.
            offset: Number of records to skip for pagination. Default: 0.
        """
        from ..functions import get_api_version
        
        api_version = get_api_version()
        if api_version == "v2":
            return {"error": "Role management is not available in Airflow 3.x (API v2)", "available_in": "v1 only"}
        
        params = []
        params.append(f"limit={limit}")
        if offset > 0:
            params.append(f"offset={offset}")
        
        query_string = "&".join(params) if params else ""
        endpoint = f"/roles?{query_string}" if query_string else "/roles"
        
        resp = await airflow_request("GET", endpoint)
        resp.raise_for_status()
        return resp.json()

    # Plugin Management (1 tool)
    @mcp.tool()
    async def list_plugins() -> Dict[str, Any]:
        """[Tool Role]: Lists all installed plugins in the Airflow system."""
        resp = await airflow_request("GET", "/plugins")
        resp.raise_for_status()
        return resp.json()

    # Provider Management (2 tools)
    @mcp.tool()
    async def list_providers() -> Dict[str, Any]:
        """[Tool Role]: Lists all provider packages installed in the Airflow system."""
        resp = await airflow_request("GET", "/providers")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_provider(provider_name: str) -> Dict[str, Any]:
        """
        [Tool Role]: Gets details of a specific provider package.

        Args:
            provider_name: Exact provider package name, such as apache-
                airflow-providers-http.
        """
        resp = await airflow_request("GET", f"/providers/{provider_name}")
        resp.raise_for_status()
        return resp.json()

    # Dataset Management (4 tools) - v1 API only (v2 uses Assets instead)
    @mcp.tool()
    async def list_datasets(limit: int = 20, offset: int = 0, uri_pattern: Optional[str] = None) -> Dict[str, Any]:
        """
        [Tool Role]: Lists all datasets in the Airflow system (v1 API only - v2 uses
        Assets).

        Args:
            limit: Maximum number of records to request per page. Default:
                20.
            offset: Number of records to skip for pagination. Default: 0.
            uri_pattern: Optional dataset URI pattern passed to the API (v1
                only). Default: None.
        """
        from ..functions import get_api_version
        
        api_version = get_api_version()
        if api_version == "v2":
            return {
                "error": "Dataset API is not available in Airflow 3.x (API v2)", 
                "available_in": "v1 only",
                "v2_alternative": "Use list_assets() for Airflow 3.x data-aware scheduling"
            }
        
        params = []
        params.append(f"limit={limit}")
        if offset > 0:
            params.append(f"offset={offset}")
        if uri_pattern:
            params.append(f"uri_pattern={uri_pattern}")
        
        query_string = "&".join(params) if params else ""
        endpoint = f"/datasets?{query_string}" if query_string else "/datasets"
        
        resp = await airflow_request("GET", endpoint)
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_dataset(dataset_uri: str) -> Dict[str, Any]:
        """
        [Tool Role]: Gets details of a specific dataset (v1 API only - v2 uses Assets).

        Args:
            dataset_uri: Dataset URI, such as s3://bucket/path; API v1 only.
        """
        from ..functions import get_api_version
        
        api_version = get_api_version()
        if api_version == "v2":
            return {
                "error": "Dataset API is not available in Airflow 3.x (API v2)", 
                "available_in": "v1 only",
                "v2_alternative": "Use Assets API for Airflow 3.x data-aware scheduling"
            }
        
        # URL encode the URI to handle special characters
        import urllib.parse
        encoded_uri = urllib.parse.quote(dataset_uri, safe='')
        
        resp = await airflow_request("GET", f"/datasets/{encoded_uri}")
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def list_dataset_events(limit: int = 20, offset: int = 0, 
                                 dataset_uri: Optional[str] = None,
                                 source_dag_id: Optional[str] = None) -> Dict[str, Any]:
        """
        [Tool Role]: Lists dataset events for data lineage tracking (v1 API only - v2
        uses Assets).

        Args:
            limit: Maximum number of records to request per page. Default:
                20.
            offset: Number of records to skip for pagination. Default: 0.
            dataset_uri: Dataset URI, such as s3://bucket/path; API v1 only.
                Default: None.
            source_dag_id: Optional identifier of the DAG that produced the
                events. Default: None.
        """
        from ..functions import get_api_version
        
        api_version = get_api_version()
        if api_version == "v2":
            return {
                "error": "Dataset events API is not available in Airflow 3.x (API v2)", 
                "available_in": "v1 only",
                "v2_alternative": "Use list_asset_events() for Airflow 3.x data lineage tracking"
            }
        
        params = []
        params.append(f"limit={limit}")
        if offset > 0:
            params.append(f"offset={offset}")
        if dataset_uri:
            params.append(f"dataset_uri={dataset_uri}")
        if source_dag_id:
            params.append(f"source_dag_id={source_dag_id}")
        
        query_string = "&".join(params) if params else ""
        endpoint = f"/datasets/events?{query_string}" if query_string else "/datasets/events"
        
        resp = await airflow_request("GET", endpoint)
        resp.raise_for_status()
        return resp.json()

    @mcp.tool()
    async def get_dataset_events(dataset_uri: str, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """
        [Tool Role]: Gets events for a specific dataset (v1 API only - v2 uses Assets).

        Args:
            dataset_uri: Dataset URI, such as s3://bucket/path; API v1 only.
            limit: Maximum number of records to request per page. Default:
                20.
            offset: Number of records to skip for pagination. Default: 0.
        """
        from ..functions import get_api_version
        
        api_version = get_api_version()
        if api_version == "v2":
            return {
                "error": "Dataset events API is not available in Airflow 3.x (API v2)", 
                "available_in": "v1 only",
                "v2_alternative": "Use list_asset_events() for Airflow 3.x data lineage tracking"
            }
        
        import urllib.parse
        encoded_uri = urllib.parse.quote(dataset_uri, safe='')
        
        params = []
        params.append(f"limit={limit}")
        if offset > 0:
            params.append(f"offset={offset}")
        
        query_string = "&".join(params) if params else ""
        endpoint = f"/datasets/{encoded_uri}/events?{query_string}" if query_string else f"/datasets/{encoded_uri}/events"
        
        resp = await airflow_request("GET", endpoint)
        resp.raise_for_status()
        return resp.json()

    logger.info("Registered all common tools (54 tools; includes v1-only operations)")
