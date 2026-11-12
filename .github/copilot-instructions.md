# Copilot Instructions for MCP-Airflow-API

## Project Architecture
**Core Pattern:** MCP (Model Context Protocol) server exposing Apache Airflow REST API as LLM-friendly tools.

**Key Components:**
- `src/mcp_airflow_api/airflow_api.py`: 40+ `@mcp.tool()` decorators defining Airflow operations
- `src/mcp_airflow_api/functions.py`: Global session management with connection pooling
- `src/mcp_airflow_api/server_instructions.md`: MCP server instructions sent to clients (cross-tool rules only, at most 4,096 characters)
- `docs/usage-guide.md`: User-facing example requests by area
- `pyproject.toml`: Entry point `mcp-airflow-api = "mcp_airflow_mcp_main:main"`

**Transport Logic:** Environment variable `FASTMCP_TYPE` controls stdio vs http mode:
```python
transport_type = args.transport_type or os.getenv("FASTMCP_TYPE", "stdio")
if transport_type == "streamable-http":
    mcp.run(transport="streamable-http", host=host, port=port)  # Docker mode
else:
    mcp.run(transport='stdio')  # Local mode
```

## Critical Patterns
**Connection Pooling:** Use global `_airflow_session` for performance:
```python
# functions.py - persistent session with retry strategy
_airflow_session = requests.Session()  # Reused across all API calls
```

**Pagination Strategy:** All tools return `{data, total_entries, limit, offset, has_more_pages, next_offset}`:
```python
# Standard pattern in airflow_api.py
def list_dags(limit=20, offset=0): 
    # Returns pagination metadata for large environments (1000+ DAGs)
```

**Configuration Files:** Multiple configs for different deployment modes:
- `mcp-config.json.stdio`: Local `python -m mcp_airflow_mcp_main`
- `mcp-config.json.http`: Docker `"url": "http://host.docker.internal:18002/mcp"`

## Docker Multi-Service Pattern
`docker-compose.yml` orchestrates 3 services:
- `mcp-server`: FastMCP server (port 18002→8080 internal)
- `mcpo-proxy`: MCP-to-REST converter (Swagger at :8002/docs)
- `open-webui`: Web interface (port 3002)

**Build Commands:** Use versioned Docker images:
```bash
./build-mcp-server-docker-image.sh  # Builds call518/mcp-server-airflow-api:1.0.0
```

## Testing Philosophy
When adding or changing a tool:
- Write its docstring and parameter descriptions; clients show these to the model.
- Add example requests to `docs/usage-guide.md`.
- Edit `server_instructions.md` only when a rule that spans several tools changes, and keep it at most 4,096 characters.

`tests/test_server_instructions.py` checks the instructions length and delivery; `tests/test_tool_documentation.py` checks tool descriptions and documentation coverage.

## Environment Requirements
**Required:** `AIRFLOW_API_BASE_URL`, `AIRFLOW_API_USERNAME`, `AIRFLOW_API_PASSWORD`
**Optional:** `AIRFLOW_API_VERSION` (defaults to 'v1'), `MCP_LOG_LEVEL` (DEBUG/INFO/WARNING/ERROR/CRITICAL), `FASTMCP_TYPE` (stdio/streamable-http), `FASTMCP_PORT` (transport control)

**Airflow Version:** Tested with 2.10.2 API v1 - all endpoints assume this version

## Development Workflow
1. Local testing: `python -m mcp_airflow_mcp_main` (stdio mode)
2. Docker development: `docker-compose up -d` then check http://localhost:8002/docs
3. Testing: `pytest tests/`

---
Key files: `airflow_api.py` (tools), `functions.py` (HTTP layer), `server_instructions.md` (LLM guidance)
