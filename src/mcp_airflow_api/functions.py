"""
Utility functions for Airflow MCP
"""
import os
import aiohttp
import asyncio
import json
from typing import Any, Dict, Optional, List

# Common constants
PROMPT_TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "prompt_template.md")

# Global session instance for connection pooling and performance optimization
_airflow_session = None
_jwt_token = None
_token_expiry = None

async def get_airflow_session() -> aiohttp.ClientSession:
    """
    Get or create a global aiohttp.ClientSession for Airflow API calls.
    This enables connection pooling and Keep-Alive connections for better performance.
    """
    global _airflow_session
    if _airflow_session is None or _airflow_session.closed:
        # Configure connection timeout and limits
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        connector = aiohttp.TCPConnector(
            limit=10,  # Total connection limit
            limit_per_host=5,  # Per-host connection limit
            keepalive_timeout=30,  # Keep connections alive
            enable_cleanup_closed=True
        )
        
        # Configure session defaults
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'mcp-airflow-api/1.0'
        }
        
        _airflow_session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers
        )
    
    return _airflow_session

def get_api_base_url():
    """Get base API URL from environment."""
    base_url = os.getenv("AIRFLOW_API_BASE_URL", "").rstrip("/")
    if not base_url:
        # Fallback to legacy AIRFLOW_API_URL for backward compatibility
        legacy_url = os.getenv("AIRFLOW_API_URL", "").rstrip("/")
        if legacy_url.endswith("/v1"):
            base_url = legacy_url[:-3]  # Remove /v1
        else:
            base_url = legacy_url
    return base_url

def get_api_version():
    """Get API version from environment."""
    return os.getenv("AIRFLOW_API_VERSION", "v1").lower()

def dag_run_date_field() -> str:
    """Return the DAG run date field name for the configured API version."""
    return "logical_date" if get_api_version() == "v2" else "execution_date"

def summarize_dag_run(run: Dict[str, Any]) -> Dict[str, Any]:
    """Map a v1 or v2 DAG run response to this server's output fields."""
    if get_api_version() == "v2":
        # v2 removed external_trigger; manual runs are the external triggers.
        external_trigger = run.get("run_type") == "manual"
    else:
        external_trigger = run.get("external_trigger")
    return {
        "run_id": run.get("dag_run_id"),
        "run_type": run.get("run_type"),
        "state": run.get("state"),
        "execution_date": run.get(dag_run_date_field()),
        "start_date": run.get("start_date"),
        "end_date": run.get("end_date"),
        "data_interval_start": run.get("data_interval_start"),
        "data_interval_end": run.get("data_interval_end"),
        "external_trigger": external_trigger,
        "conf": run.get("conf"),
        "note": run.get("note")
    }

def version_dag_fields(dag: Dict[str, Any]) -> Dict[str, Any]:
    """Return DAG fields whose names differ between API v1 and v2."""
    if get_api_version() != "v2":
        return {
            "is_active": dag.get("is_active"),
            "schedule_interval": dag.get("schedule_interval"),
            "next_dagrun": dag.get("next_dagrun")
        }
    is_stale = dag.get("is_stale")
    return {
        "is_active": None if is_stale is None else not is_stale,
        "schedule_interval": dag.get("timetable_summary"),
        "next_dagrun": dag.get("next_dagrun_logical_date")
    }

def construct_api_url(path: str) -> str:
    """Construct full API URL with version."""
    base_url = get_api_base_url()
    version = get_api_version()
    
    if not base_url:
        raise RuntimeError("AIRFLOW_API_BASE_URL environment variable is not set. " +
                         "Please set AIRFLOW_API_BASE_URL (e.g., 'http://localhost:8080/api') and " +
                         "optionally AIRFLOW_API_VERSION (defaults to 'v1')")
    
    # Ensure path starts with /
    if not path.startswith("/"):
        path = "/" + path
    
    return f"{base_url}/{version}{path}"

async def get_jwt_token() -> str:
    """
    Get JWT token for Airflow 3.x authentication.
    Caches the token and automatically refreshes when needed.
    """
    global _jwt_token, _token_expiry
    import time
    
    # Check if we have a valid cached token
    if _jwt_token and _token_expiry and time.time() < _token_expiry:
        return _jwt_token
    
    # Get credentials
    username = os.getenv("AIRFLOW_API_USERNAME")
    password = os.getenv("AIRFLOW_API_PASSWORD")
    if not username or not password:
        raise RuntimeError("AIRFLOW_API_USERNAME and AIRFLOW_API_PASSWORD required for JWT token authentication")
    
    # Get base URL for token endpoint
    base_url = get_api_base_url()
    if not base_url:
        raise RuntimeError("AIRFLOW_API_BASE_URL environment variable is not set. " +
                         "Please set AIRFLOW_API_BASE_URL (e.g., 'http://localhost:8080/api') and " +
                         "optionally AIRFLOW_API_VERSION (defaults to 'v1')")
    
    # Remove /api suffix for auth endpoint
    auth_base_url = base_url.replace("/api", "")
    token_url = f"{auth_base_url}/auth/token"
    
    # Request token
    session = await get_airflow_session()
    payload = {
        "username": username,
        "password": password
    }
    
    async with session.post(token_url, json=payload) as response:
        if response.status in [200, 201]:  # Accept both 200 OK and 201 Created
            data = await response.json()
            _jwt_token = data.get("access_token")
            # Set expiry to 23 hours from now (tokens typically last 24 hours)
            _token_expiry = time.time() + (23 * 60 * 60)
            return _jwt_token
        else:
            error_text = await response.text()
            raise RuntimeError(f"Failed to obtain JWT token: {response.status} - {error_text}")

async def airflow_request(method: str, path: str, **kwargs) -> aiohttp.ClientResponse:
    """
    Make an authenticated request to Airflow REST API using a persistent session.
    Automatically selects the appropriate authentication method based on API version:
    - v1 API (Airflow 2.x): Basic Auth using username/password
    - v2 API (Airflow 3.x): Auto JWT Token (internally managed) with Basic Auth fallback
    
    Requires only AIRFLOW_API_USERNAME and AIRFLOW_API_PASSWORD environment variables.
    JWT tokens are automatically obtained and cached for v2 API.
    
    'path' should be relative to API version (e.g., '/dags', '/pools').
    URL construction: {AIRFLOW_API_BASE_URL}/{AIRFLOW_API_VERSION}{path}
    """
    # Use new version-aware URL construction
    full_url = construct_api_url(path)
    
    # Get authentication credentials
    username = os.getenv("AIRFLOW_API_USERNAME")
    password = os.getenv("AIRFLOW_API_PASSWORD")
    api_version = get_api_version()
    
    if not username or not password:
        raise RuntimeError(f"Authentication required for API {api_version}: Both AIRFLOW_API_USERNAME and AIRFLOW_API_PASSWORD environment variables must be set")
    
    headers = kwargs.pop("headers", {})
    auth = None
    
    if api_version == "v2":
        # Auto JWT Token for Airflow 3.x (v2 API) - internally managed
        try:
            jwt_token = await get_jwt_token()
            headers["Authorization"] = f"Bearer {jwt_token}"
        except Exception as e:
            # Fallback to Basic Auth if JWT fails (for hybrid environments)
            print(f"JWT token failed for v2 API, falling back to Basic Auth: {e}")
            auth = aiohttp.BasicAuth(username, password)
    elif api_version == "v1":
        # Basic Auth authentication for Airflow 2.x (v1 API)
        auth = aiohttp.BasicAuth(username, password)
    else:
        raise RuntimeError(f"Unsupported API version: {api_version}. Supported versions: v1, v2")
    
    # Use persistent session for better performance
    session = await get_airflow_session()
    
    async with session.request(method, full_url, headers=headers, auth=auth, **kwargs) as response:
        # Store response data before context manager closes
        response_data = await response.text()
        response_status = response.status
        response_headers = dict(response.headers)
        
        # Create a response-like object
        class AsyncResponse:
            def __init__(self, status, text, headers, url):
                self.status_code = status
                self._text = text
                self._headers = headers
                self.headers = headers
                self.url = url
                self.real_url = url  # Add real_url attribute
            
            def raise_for_status(self):
                if self.status_code >= 400:
                    raise RuntimeError(f"HTTP {self.status_code}: {self._text}")
            
            def json(self):
                import json
                return json.loads(self._text)
            
            @property
            def text(self):
                return self._text
        
        return AsyncResponse(response_status, response_data, response_headers, full_url)

async def airflow_request_v1(method: str, path: str, **kwargs) -> aiohttp.ClientResponse:
    """Make API v1 request."""
    # Temporarily override version for this request
    original_version = os.getenv("AIRFLOW_API_VERSION")
    os.environ["AIRFLOW_API_VERSION"] = "v1"
    try:
        return await airflow_request(method, path, **kwargs)
    finally:
        # Restore original version
        if original_version:
            os.environ["AIRFLOW_API_VERSION"] = original_version
        elif "AIRFLOW_API_VERSION" in os.environ:
            del os.environ["AIRFLOW_API_VERSION"]

async def airflow_request_v2(method: str, path: str, **kwargs) -> aiohttp.ClientResponse:
    """Make API v2 request."""
    # Temporarily override version for this request
    original_version = os.getenv("AIRFLOW_API_VERSION")
    os.environ["AIRFLOW_API_VERSION"] = "v2"
    try:
        return await airflow_request(method, path, **kwargs)
    finally:
        # Restore original version
        if original_version:
            os.environ["AIRFLOW_API_VERSION"] = original_version
        elif "AIRFLOW_API_VERSION" in os.environ:
            del os.environ["AIRFLOW_API_VERSION"]

async def close_airflow_session():
    """
    Close the global Airflow session and cleanup resources.
    This is optional and mainly useful for testing or application shutdown.
    """
    global _airflow_session
    if _airflow_session is not None:
        await _airflow_session.close()
        _airflow_session = None

def read_prompt_template(path: str) -> str:
    """
    Reads the MCP prompt template file and returns its content as a string.
    """
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def parse_prompt_sections(template: str):
    """
    Parses the prompt template into section headings and sections.
    Returns (headings, sections).
    """
    lines = template.splitlines()
    sections = []
    current = []
    headings = []
    for line in lines:
        if line.startswith("## "):
            if current:
                sections.append("\n".join(current))
                current = []
            headings.append(line[3:].strip())
            current.append(line)
        else:
            current.append(line)
    if current:
        sections.append("\n".join(current))
    return headings, sections


def get_current_time_context() -> Dict[str, Any]:
    """
    Internal helper: Returns the current time context for relative date calculations.

    Returns:
        Current date and time information for reference in date calculations
    """
    from datetime import datetime, timedelta
    current_time = datetime.now()
    current_date_str = current_time.strftime('%Y-%m-%d')

    # Calculate relative dates based on actual current time
    yesterday = (current_time - timedelta(days=1)).strftime('%Y-%m-%d')
    last_week_start = (current_time - timedelta(days=7)).strftime('%Y-%m-%d')
    last_week_end = (current_time - timedelta(days=1)).strftime('%Y-%m-%d')
    last_3_days_start = (current_time - timedelta(days=3)).strftime('%Y-%m-%d')

    return {
        "current_date": current_date_str,
        "current_time": current_time.strftime('%Y-%m-%d %H:%M:%S'),
        "reference_date": f"{current_time.strftime('%B %d, %Y')} ({current_date_str})",
        "date_calculation_examples": {
            "yesterday": yesterday,
            "last_week": f"{last_week_start} to {last_week_end}",
            "last_3_days": f"{last_3_days_start} to {current_date_str}",
            "today": current_date_str
        }
    }

# DAG-related helper functions
async def list_dags_internal(limit: int = 20,
                      offset: int = 0,
                      fetch_all: bool = False,
                      id_contains: Optional[str] = None,
                      name_contains: Optional[str] = None) -> Dict[str, Any]:
    """
    Internal helper function to list DAGs.
    This function contains the actual implementation logic that can be reused.
    """
    # Helper: server-side filtering by ID and display name
    def _filter_dags(dag_list):
        results = dag_list
        if id_contains:
            key = id_contains.lower()
            results = [d for d in results if key in d.get("dag_id", "").lower()]
        if name_contains:
            key = name_contains.lower()
            results = [d for d in results if key in (d.get("dag_display_name") or "").lower()]
        return results

    # If fetch_all=True, loop through pages to collect all DAGs
    if fetch_all:
        all_dags = []
        current_offset = offset
        total_entries = None
        pages_fetched = 0
        while True:
            # recursive call without fetch_all to fetch one page
            result = await list_dags_internal(limit=limit, offset=current_offset)
            page_dags = result.get("dags", [])
            all_dags.extend(page_dags)
            pages_fetched += 1
            total_entries = result.get("total_entries", 0)
            if not result.get("has_more_pages", False) or not page_dags:
                break
            current_offset = result.get("next_offset", current_offset + limit)
        # apply filters
        filtered = _filter_dags(all_dags)
        return {
            "dags": filtered,
            "total_entries": len(filtered),
            "pages_fetched": pages_fetched,
            "limit": limit,
            "offset": offset
        }
    # Default: paginated fetch
    params = []
    params.append(f"limit={limit}")
    if offset > 0:
        params.append(f"offset={offset}")
    
    query_string = "&".join(params) if params else ""
    endpoint = f"/dags?{query_string}" if query_string else "/dags"
    
    resp = await airflow_request("GET", endpoint)
    resp.raise_for_status()
    response_data = resp.json()
    dags = response_data.get("dags", [])
    dag_list = []
    for dag in dags:
        version_fields = version_dag_fields(dag)
        # Extract schedule interval info
        schedule_info = version_fields["schedule_interval"]
        if isinstance(schedule_info, dict) and schedule_info.get("__type") == "CronExpression":
            schedule_display = schedule_info.get("value", "Unknown")
        elif schedule_info:
            schedule_display = str(schedule_info)
        else:
            schedule_display = None
        
        dag_info = {
            "dag_id": dag.get("dag_id"),
            "dag_display_name": dag.get("dag_display_name"),
            "description": dag.get("description"),
            "is_active": version_fields["is_active"],
            "is_paused": dag.get("is_paused"),
            "schedule_interval": schedule_display,
            "max_active_runs": dag.get("max_active_runs"),
            "max_active_tasks": dag.get("max_active_tasks"),
            "owners": dag.get("owners"),
            "tags": [t.get("name") for t in dag.get("tags", [])],
            "next_dagrun": version_fields["next_dagrun"],
            "next_dagrun_data_interval_start": dag.get("next_dagrun_data_interval_start"),
            "next_dagrun_data_interval_end": dag.get("next_dagrun_data_interval_end"),
            "last_parsed_time": dag.get("last_parsed_time"),
            "has_import_errors": dag.get("has_import_errors"),
            "has_task_concurrency_limits": dag.get("has_task_concurrency_limits"),
            "timetable_description": dag.get("timetable_description"),
            "fileloc": dag.get("fileloc"),
            "file_token": dag.get("file_token")
        }
        dag_list.append(dag_info)
    
    # Calculate pagination info and apply filters
    total_entries = response_data.get("total_entries", len(dag_list))
    has_more_pages = (offset + limit) < total_entries
    next_offset = offset + limit if has_more_pages else None
    filtered = _filter_dags(dag_list)
    returned_count = len(filtered)
    
    return {
        "dags": filtered,
        "total_entries": total_entries,
        "limit": limit,
        "offset": offset,
        "returned_count": returned_count,
        "has_more_pages": has_more_pages,
        "next_offset": next_offset,
        "pagination_info": {
            "current_page": (offset // limit) + 1 if limit > 0 else 1,
            "total_pages": ((total_entries - 1) // limit) + 1 if limit > 0 and total_entries > 0 else 1,
            "remaining_count": max(0, total_entries - (offset + returned_count))
        }
    }

async def get_dag_detailed_info(dag_id: str) -> Dict[str, Any]:
    """
    Internal helper function to get detailed DAG information.
    This function contains the actual implementation logic that can be reused.
    """
    if not dag_id:
        raise ValueError("dag_id must not be empty")
    # start_date, end_date, and catchup exist only in the details response.
    resp = await airflow_request("GET", f"/dags/{dag_id}/details")
    resp.raise_for_status()
    dag = resp.json()
    version_fields = version_dag_fields(dag)
    return {
        "dag_id": dag.get("dag_id"),
        "dag_display_name": dag.get("dag_display_name"),
        "description": dag.get("description"),
        "schedule_interval": version_fields["schedule_interval"],
        "start_date": dag.get("start_date"),
        "end_date": dag.get("end_date"),
        "is_active": version_fields["is_active"],
        "is_paused": dag.get("is_paused"),
        "owners": dag.get("owners"),
        "tags": [t.get("name") for t in dag.get("tags", [])],
        "catchup": dag.get("catchup"),
        "max_active_runs": dag.get("max_active_runs"),
        "max_active_tasks": dag.get("max_active_tasks"),
        "has_task_concurrency_limits": dag.get("has_task_concurrency_limits"),
        "has_import_errors": dag.get("has_import_errors"),
        "next_dagrun": version_fields["next_dagrun"],
        "next_dagrun_data_interval_start": dag.get("next_dagrun_data_interval_start"),
        "next_dagrun_data_interval_end": dag.get("next_dagrun_data_interval_end")
    }
