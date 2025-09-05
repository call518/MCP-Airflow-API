# Copilot Instructions for MCP-Airflow-API

## Project Architecture
**Core Pattern:** MCP (Model Context Protocol) server exposing Apache Airflow REST API as LLM-friendly tools.

**Key Components:**
- `src/mcp_airflow_api/airflow_api.py`: 40+ `@mcp.tool()` decorators defining Airflow operations
- `src/mcp_airflow_api/functions.py`: Global session management with connection pooling
- `src/mcp_airflow_api/prompt_template.md`: Canonical English instructions for LLMs
- `pyproject.toml`: Entry point `mcp-airflow-api = "mcp_airflow_api.airflow_api:main"`

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
- `mcp-config.json.stdio`: Local `python -m mcp_airflow_api.airflow_api`
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
Tests in `tests/test_prompt_template.py` are "resilient to template content changes":
- Use `TEMPLATE_CONFIG` constants for validation thresholds
- Test structure/behavior, not exact content
- Fixture-based template loading for performance

## Environment Requirements
**Required:** `AIRFLOW_API_URL`, `AIRFLOW_API_USERNAME`, `AIRFLOW_API_PASSWORD`
**Optional:** `MCP_LOG_LEVEL` (DEBUG/INFO/WARNING/ERROR/CRITICAL), `FASTMCP_TYPE` (stdio/streamable-http), `FASTMCP_PORT` (transport control)

**Airflow Version:** Tested with 2.10.2 API v1 - all endpoints assume this version

## Development Workflow
1. Local testing: `python -m mcp_airflow_api.airflow_api` (stdio mode)
2. Docker development: `docker-compose up -d` then check http://localhost:8002/docs
3. Testing: `pytest tests/` (uses resilient template validation)

---
Key files: `airflow_api.py` (tools), `functions.py` (HTTP layer), `prompt_template.md` (LLM guidance)
