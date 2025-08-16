[![MSeeP.ai Security Assessment Badge](https://mseep.net/pr/call518-mcp-airflow-api-badge.png)](https://mseep.ai/app/call518-mcp-airflow-api)

[![Verified on MSeeP](https://mseep.ai/badge.svg)](https://mseep.ai/app/d024d598-d442-4e4e-827b-d976e4d372fb)

[![Deploy to PyPI with tag](https://github.com/call518/MCP-Airflow-API/actions/workflows/pypi-publish.yml/badge.svg)](https://github.com/call518/MCP-Airflow-API/actions/workflows/pypi-publish.yml)

[![smithery badge](https://smithery.ai/badge/@call518/mcp-airflow-api)](https://smithery.ai/server/@call518/mcp-airflow-api)

Model Context Protocol (MCP) server for Apache Airflow API integration.  
This project provides natural language MCP tools for essential Airflow cluster operations.

> **Note:** To minimize operational risk, this MCP server currently focuses on read-only (query) operations only. APIs that modify the target Airflow cluster (e.g., triggering or pausing DAGs) are planned but currently on hold.

---

# MCP-Airflow-API

**Tested and supported Airflow version: 2.10.2 (API Version: v1)**

- [Airflow API Documents](https://airflow.apache.org/docs/apache-airflow/2.0.0/stable-rest-api-ref.html)

## Example Query - List DAGs

![ScreenShot-009](img/screenshot-009.png)

## Usages

Using this is very simple and straightforward. If you already have an MCP Tools environment running, just add the following configuration to your `mcp-config.json` file:

```json
{
  "mcpServers": {
    "airflow-api": {
      "command": "uvx",
      "args": ["--python", "3.11", "mcp-airflow-api"],
      "env": {
        "AIRFLOW_API_URL": "http://localhost:8080/api/v1",
        "AIRFLOW_API_USERNAME": "airflow",
        "AIRFLOW_API_PASSWORD": "airflow",
        "AIRFLOW_LOG_LEVEL": "INFO"
      }
    }
  }
}
```

## Features

- List all DAGs in the Airflow cluster
- Monitor running/failed DAG runs  
- Trigger DAG runs on demand
- Check cluster health and version information
- Minimal, LLM-friendly output for all tools
- Easy integration with MCP Inspector, OpenWebUI, Smithery, etc.
- **Enhanced for Large-Scale Environments**: Improved default limits and pagination support for enterprise Airflow deployments (100+ to 1000+ DAGs)

## Resource-Optimized Design

This MCP server has been optimized for efficient resource usage and better performance:

- **Optimized Default Limits**: Most functions use default limits of 20 (reduced from 100) for better memory usage and faster response times
- **Comprehensive Pagination**: All listing functions include pagination metadata (`has_more_pages`, `next_offset`, `pagination_info`)
- **Automatic Pagination Helpers**: Use `list_dags(..., fetch_all=True)` for complete DAG inventory without manual pagination
- **Flexible Scaling**: Users can specify higher limits (up to 1000) when needed for bulk operations
- **Memory-Efficient**: Smaller default payloads reduce memory usage while maintaining full functionality

---

## Available MCP Tools

### DAG Management

- `list_dags(limit=20, offset=0, fetch_all=False, id_contains=None, name_contains=None)`  
	Returns all DAGs registered in the Airflow cluster with pagination support.  
	Output: `dag_id`, `dag_display_name`, `is_active`, `is_paused`, `owners`, `tags`, plus pagination info (`total_entries`, `limit`, `offset`, `has_more_pages`, `next_offset`, `pagination_info`)

	**Pagination Examples:**
	- First 20 DAGs: `list_dags()`
	- Next 20 DAGs: `list_dags(limit=20, offset=20)`
	- Large batch: `list_dags(limit=100, offset=0)`
	- All DAGs at once: `list_dags(limit=1000)`

	- `id_contains="etl"` → Only DAGs whose `dag_id` contains "etl"
	- `name_contains="daily"` → Only DAGs whose `display_name` contains "daily"
	- If both are specified, only DAGs matching both conditions are returned

- `running_dags`  
	Returns all currently running DAG runs.  
	Output: `dag_id`, `run_id`, `state`, `execution_date`, `start_date`, `end_date`

- `failed_dags`  
	Returns all recently failed DAG runs.  
	Output: `dag_id`, `run_id`, `state`, `execution_date`, `start_date`, `end_date`

- `trigger_dag(dag_id)`  
	Immediately triggers the specified DAG.  
	Output: `dag_id`, `run_id`, `state`, `execution_date`, `start_date`, `end_date`

- `pause_dag(dag_id)`  
	Pauses the specified DAG (prevents scheduling new runs).  
	Output: `dag_id`, `is_paused`

- `unpause_dag(dag_id)`  
	Unpauses the specified DAG (allows scheduling new runs).  
	Output: `dag_id`, `is_paused`

### Cluster Management & Health

- `get_health`  
	Get the health status of the Airflow webserver instance.  
	Output: `metadatabase`, `scheduler`, overall health `status`

- `get_version`  
	Get version information of the Airflow instance.  
	Output: `version`, `git_version`, `build_date`, `api_version`

### Pool Management

- `list_pools(limit=20, offset=0)`  
	List all pools in the Airflow instance with pagination support.  
	Output: `pools`, `total_entries`, `limit`, `offset`, pool details with slots usage

- `get_pool(pool_name)`  
	Get detailed information about a specific pool.  
	Output: `name`, `slots`, `occupied_slots`, `running_slots`, `queued_slots`, `open_slots`, `description`, `utilization_percentage`

### Variable Management

- `list_variables(limit=20, offset=0, order_by="key")`  
	List all variables stored in Airflow with pagination support.  
	Output: `variables`, `total_entries`, `limit`, `offset`, variable details with keys, values, and descriptions

- `get_variable(variable_key)`  
	Get detailed information about a specific variable by its key.  
	Output: `key`, `value`, `description`, `is_encrypted`

### Task Instance Management

- `list_task_instances_all(dag_id=None, dag_run_id=None, execution_date_gte=None, execution_date_lte=None, start_date_gte=None, start_date_lte=None, end_date_gte=None, end_date_lte=None, duration_gte=None, duration_lte=None, state=None, pool=None, queue=None, limit=20, offset=0)`  
	Lists task instances across all DAGs or filtered by specific criteria with comprehensive filtering options.  
	Output: `task_instances`, `total_entries`, `limit`, `offset`, `applied_filters`

- `get_task_instance_details(dag_id, dag_run_id, task_id)`  
	Retrieves detailed information about a specific task instance.  
	Output: Comprehensive task instance details including execution info, state, timing, configuration, and metadata

- `list_task_instances_batch(dag_ids=None, dag_run_ids=None, task_ids=None, execution_date_gte=None, execution_date_lte=None, start_date_gte=None, start_date_lte=None, end_date_gte=None, end_date_lte=None, duration_gte=None, duration_lte=None, state=None, pool=None, queue=None)`  
	Lists task instances in batch with multiple filtering criteria for bulk operations.  
	Output: `task_instances`, `total_entries`, `applied_filters`, batch processing results

- `get_task_instance_extra_links(dag_id, dag_run_id, task_id)`  
	Lists extra links for a specific task instance (e.g., monitoring dashboards, logs, external resources).  
	Output: `task_id`, `dag_id`, `dag_run_id`, `extra_links`, `total_links`

- `get_task_instance_logs(dag_id, dag_run_id, task_id, try_number=1, full_content=False, token=None)`  
	Retrieves logs for a specific task instance and its try number with content and metadata.  
	Output: `task_id`, `dag_id`, `dag_run_id`, `try_number`, `content`, `continuation_token`, `metadata`

### XCom Management

- `list_xcom_entries(dag_id, dag_run_id, task_id, limit=20, offset=0)`  
	Lists XCom entries for a specific task instance.  
	Output: `dag_id`, `dag_run_id`, `task_id`, `xcom_entries`, `total_entries`, `limit`, `offset`

- `get_xcom_entry(dag_id, dag_run_id, task_id, xcom_key, map_index=-1)`  
	Retrieves a specific XCom entry for a task instance.  
	Output: `dag_id`, `dag_run_id`, `task_id`, `xcom_key`, `map_index`, `key`, `value`, `timestamp`, `execution_date`, `run_id`

### DAG Analysis & Monitoring

- `get_dag(dag_id)`  
	Retrieves comprehensive details for a specific DAG.  
	Output: `dag_id`, `description`, `schedule_interval`, `owners`, `tags`, `start_date`, `next_dagrun`, etc.

- `dag_graph(dag_id)`  
	Retrieves task dependency graph structure for a specific DAG.  
	Output: `dag_id`, `tasks`, `dependencies`, task relationships

- `list_tasks(dag_id)`  
	Lists all tasks for a specific DAG.  
	Output: `dag_id`, `tasks`, task configuration details  
	Output: `dag_id`, `tasks`, `dependencies`, task relationships

- `dag_code(dag_id)`  
	Retrieves the source code for a specific DAG.  
	Output: `dag_id`, `file_token`, `source_code`

- `list_event_logs(dag_id=None, task_id=None, run_id=None, limit=20, offset=0)`  
	Lists event log entries with optional filtering.  
	**Optimized limit**: Default is 20 for better performance while maintaining good coverage.  
	Output: `event_logs`, `total_entries`, `limit`, `offset`, `has_more_pages`, `next_offset`, `pagination_info`

- `get_event_log(event_log_id)`  
	Retrieves a specific event log entry by ID.  
	Output: `event_log_id`, `when`, `event`, `dag_id`, `task_id`, `run_id`, etc.

- `all_dag_event_summary()`  
	Retrieves event count summary for all DAGs.  
	**Improved limit**: Uses limit=1000 for DAG retrieval to avoid missing DAGs in large environments.  
	Output: `dag_summaries`, `total_dags`, `total_events`

- `list_import_errors(limit=20, offset=0)`  
	Lists import errors with optional filtering.  
	**Optimized limit**: Default is 20 for better performance while maintaining good coverage.  
	Output: `import_errors`, `total_entries`, `limit`, `offset`, `has_more_pages`, `next_offset`, `pagination_info`

- `get_import_error(import_error_id)`  
	Retrieves a specific import error by ID.  
	Output: `import_error_id`, `filename`, `stacktrace`, `timestamp`

- `all_dag_import_summary()`  
	Retrieves import error summary for all DAGs.  
	Output: `import_summaries`, `total_errors`, `affected_files`

- `dag_run_duration(dag_id, limit=50)`  
	Retrieves run duration statistics for a specific DAG.  
	**Improved limit**: Default increased from 10 to 50 for better statistical analysis.  
	Output: `dag_id`, `runs`, duration analysis, success/failure stats

- `dag_task_duration(dag_id, run_id=None)`  
	Retrieves task duration information for a specific DAG run.  
	Output: `dag_id`, `run_id`, `tasks`, individual task performance

- `dag_calendar(dag_id, start_date=None, end_date=None, limit=20)`  
	Retrieves calendar/schedule information for a specific DAG.  
	**Configurable limit**: Default is 20, can be increased up to 1000 for bulk analysis.  
	Output: `dag_id`, `schedule_interval`, `runs`, upcoming executions

---

## Example Queries

### Basic DAG Operations
- **list_dags**: "List all DAGs with limit 10 in a table format." → Returns up to 10 DAGs
- **list_dags**: "List all DAGs a table format." → Returns up to All DAGs (WARN: Need High Tokens)
- **list_dags**: "Show next page of DAGs." → Use offset for pagination
- **list_dags**: "List DAGs 21-40." → `list_dags(limit=20, offset=20)`
- **list_dags**: "Filter DAGs whose ID contains 'tutorial'." → `list_dags(id_contains="etl")`
- **list_dags**: "Filter DAGs whose display name contains 'tutorial'." → `list_dags(name_contains="daily")`
- **running_dags**: "Show running DAGs."
- **failed_dags**: "Show failed DAGs."
- **trigger_dag**: "Trigger DAG 'example_complex'."
- **pause_dag**: "Pause DAG 'example_complex' in a table format."
- **unpause_dag**: "Unpause DAG 'example_complex' in a table format."

### Cluster Management & Health
- **get_health**: "Check Airflow cluster health."
- **get_version**: "Get Airflow version information."

### Pool Management
- **list_pools**: "List all pools."
- **list_pools**: "Show pool usage statistics."
- **get_pool**: "Get details for pool 'default_pool'."
- **get_pool**: "Check pool utilization."

### Variable Management
- **list_variables**: "List all variables."
- **list_variables**: "Show all Airflow variables with their values."
- **get_variable**: "Get variable 'database_url'."
- **get_variable**: "Show the value of variable 'api_key'."

### Task Instance Management
- **list_task_instances_all**: "List all task instances for DAG 'example_complex'."
- **list_task_instances_all**: "Show running task instances."
- **list_task_instances_all**: "Show task instances filtered by pool 'default_pool'."
- **list_task_instances_all**: "List task instances with duration greater than 300 seconds."
- **list_task_instances_all**: "Show failed task instances from last week."
- **list_task_instances_all**: "List failed task instances from yesterday."
- **list_task_instances_all**: "Show task instances that started after 9 AM today."
- **list_task_instances_all**: "List task instances from the last 3 days with state 'failed'."
- **get_task_instance_details**: "Get details for task 'data_processing' in DAG 'example_complex' run 'scheduled__xxxxx'."
- **list_task_instances_batch**: "List failed task instances from last month."
- **list_task_instances_batch**: "Show task instances in batch for multiple DAGs from this week."
- **get_task_instance_extra_links**: "Get extra links for task 'data_processing' in latest run."
- **get_task_instance_logs**: "Retrieve logs for task 'create_entry_gcs' try number 2 of DAG 'example_complex'."

### XCom Management
- **list_xcom_entries**: "List XCom entries for task 'data_processing' in DAG 'example_complex' run 'scheduled__xxxxx'."
- **list_xcom_entries**: "Show all XCom entries for task 'data_processing' in latest run."
- **get_xcom_entry**: "Get XCom entry with key 'result' for task 'data_processing' in specific run."
- **get_xcom_entry**: "Retrieve XCom value for key 'processed_count' from task 'data_processing'."

### DAG Analysis & Monitoring
- **get_dag**: "Get details for DAG 'example_complex'."
- **dag_graph**: "Show task graph for DAG 'example_complex'."
- **list_tasks**: "List all tasks in DAG 'example_complex'."
- **dag_code**: "Get source code for DAG 'example_complex'."
- **list_event_logs**: "List event logs for DAG 'example_complex'."
- **list_event_logs**: "Show event logs with ID from yesterday for all DAGs."
- **get_event_log**: "Get event log entry with ID 12345."
- **all_dag_event_summary**: "Show event count summary for all DAGs."
- **list_import_errors**: "List import errors with ID."
- **get_import_error**: "Get import error with ID 67890."
- **all_dag_import_summary**: "Show import error summary for all DAGs."
- **dag_run_duration**: "Get run duration stats for DAG 'example_complex'."
- **dag_task_duration**: "Show latest run of DAG 'example_complex'."
- **dag_task_duration**: "Show task durations for latest run of 'manual__xxxxx'."
- **dag_calendar**: "Get calendar info for DAG 'example_complex' from last month."
- **dag_calendar**: "Show DAG schedule for 'example_complex' from this week."

## Date Calculation Guidance

Relative date phrases like "yesterday" or "last week" are resolved against the server's current date/time internally by the tools. You do not need to call any separate tool.

| User Input | Calculation Method | Example Format |
|------------|-------------------|----------------|
| "yesterday" | current_date - 1 day | YYYY-MM-DD (1 day before current) |
| "last week" | current_date - 7 days to current_date - 1 day | YYYY-MM-DD to YYYY-MM-DD (7 days range) |
| "last 3 days" | current_date - 3 days to current_date | YYYY-MM-DD to YYYY-MM-DD (3 days range) |
| "this morning" | current_date 00:00 to 12:00 | YYYY-MM-DDTHH:mm:ssZ format |

The server always bases calculations on its actual current date/time.

## Formatting Rules

- Output only the requested fields.
- No extra explanation unless explicitly requested.
- Use JSON objects for tool outputs.

---

## Prompt Template

The package exposes a tool `get_prompt_template` that returns either the entire template, a specific section, or just the headings. Three MCP prompts (`prompt_template_full`, `prompt_template_headings`, `prompt_template_section`) are also registered for discovery.

### MCP Prompts

For easier discoverability in MCP clients (so `prompts/list` is not empty), the server now registers three prompts:

• `prompt_template_full` – returns the full canonical template  
• `prompt_template_headings` – returns only the section headings  
• `prompt_template_section` – takes a `section` argument (number or keyword) and returns that section

You can still use the `get_prompt_template` tool for programmatic access or when you prefer tool invocation over prompt retrieval.

Single canonical English prompt template guides safe and efficient tool selection.

Files:
• Packaged: `src/mcp_airflow_api/prompt_template.md` (distributed with PyPI)  
• (Optional workspace root copy `PROMPT_TEMPLATE.md` may exist for editing; packaged copy is the one loaded at runtime.)

Retrieve dynamically via MCP tool:
• `get_prompt_template()` – full template  
• `get_prompt_template("tool map")` – only the tool mapping section  
• `get_prompt_template("3")` – section 3 (tool map)  
• `get_prompt_template(mode="headings")` – list all section headings

Policy: Only English is stored; LLM는 사용자 질의 언어와 무관하게 영어 지침을 내부 추론용으로 사용하고, 사용자 응답은 필요 시 다국어로 생성한다.

---

## Main Tool Files

- MCP tool definitions: `src/mcp_airflow_api/airflow_api.py`
- Utility functions: `src/mcp_airflow_api/functions.py`

---

## Prompt Template

## How To Use

1. In your MCP Tools environment, configure `mcp-config.json` as follows:

```json
{
	"mcpServers": {
		"airflow-api": {
			"command": "uvx",
			"args": ["--python", "3.11", "mcp-airflow-api"],
			"env": {
				"AIRFLOW_API_URL": "http://localhost:38080/api/v1",
				"AIRFLOW_API_USERNAME": "airflow",
				"AIRFLOW_API_PASSWORD": "airflow",
				"AIRFLOW_LOG_LEVEL": "INFO"
			}
		}
	}
}
```

2. Register the MCP server in MCP Inspector, OpenWebUI, Smithery, etc. and use the tools.

---

## QuickStart (Demo): Running MCP-Airflow-API with Docker

1. Prepare an Airflow Demo cluster  
	- Try this, [Airflow-Docker-Compose](https://github.com/call518/Airflow-Docker-Compose)
	- (Optional) See [Official Airflow Docker Install Guide](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)

2. Prepare MCP Tools environment
	- Install Docker and Docker Compose
	- Edit `mcp-config.json` file for AIRFLOW Cluster Info.
	- Clone this project and run `docker-compose up -d` in the root directory

3. Check MCP-Server REST-API (by MCPO Swagger)
	- Try check this URL: http://localhost:8002/docs

4. Access to OpenWebUI with your browser
	- (NOTE) Port numbers can change due to conflicts — check your docker-compose.yml.
	- Example OpenWebUI address: `http://localhost:8000/airflow-api`

5. Register the MCP server
	- In [Settings] — [Tools], add the API address of the “airflow-api” tool (the link displayed in the MCPO Swagger), e.g., http://localhost:8001/airflow-api

6. Setup LLM
	- In [Admin Pannel] - [Setting] - [Connection], configure API Key for OpenAI or Ollama.

7. Completed!

---

## Pagination Guide for Large Airflow Environments

### Understanding DAG Pagination

The `list_dags()` function now supports pagination to handle large Airflow environments efficiently:

**Default Behavior:**
- Returns first 100 DAGs by default
- Includes pagination metadata in response

**Pagination Response Structure:**
```json
{
  "dags": [...],
  "total_entries": 1500,
  "limit": 100,
  "offset": 0,
  "returned_count": 100,
  "has_more_pages": true,
  "next_offset": 100,
  "pagination_info": {
    "current_page": 1,
    "total_pages": 15,
    "remaining_count": 1400
  }
}
```

### Pagination Strategies

**🔍 Exploratory (Recommended for LLMs):**
```
1. list_dags() → Check first 20 DAGs
2. Use has_more_pages to determine if more exist
3. list_dags(limit=20, offset=20) → Get next 20
4. Continue as needed
```

**📊 Complete Analysis:**
```
→ Automatically fetches ALL DAGs regardless of count
```

**⚡ Quick Large Queries:**
```
list_dags(limit=500)
→ Get up to 500 DAGs in one call
```

### Best Practices

- **Small Airflow (< 50 DAGs)**: Use default `list_dags()`
- **Medium Airflow (50-500 DAGs)**: Use `list_dags(limit=100)` or `list_dags(limit=200)`  
- **Memory-conscious**: Use default limits (20) with manual pagination

---

## Logging & Observability

- Structured logs for all tool invocations and HTTP requests
- Control log level via environment variable (`AIRFLOW_LOG_LEVEL`) or CLI flag (`--log-level`)
- Supported levels: DEBUG, INFO, WARNING, ERROR, CRITICAL

---

## Installing via Smithery

To install Airflow API Integration Server for Claude Desktop automatically via [Smithery](https://smithery.ai/server/@call518/mcp-airflow-api):

```bash
npx -y @smithery/cli install @call518/mcp-airflow-api --client claude
```

- [Issues](https://github.com/call518/MCP-Airflow-API/issues)
- [Smithery Deployment](https://smithery.ai/server/@call518/mcp-airflow-api)

---

## Roadmap

This project starts with a minimal set of essential Airflow management tools. Many more useful features and tools for Airflow cluster operations will be added soon, including advanced monitoring, DAG/task analytics, scheduling controls, and more. Contributions and suggestions are welcome!

---

## Additional Links

- [Code](https://github.com/call518/MCP-Airflow-API)

---

## Testing

This project includes comprehensive tests for the prompt template functionality.

### Running Tests

```bash
# Install development dependencies
uv sync

# Run all tests
uv run pytest

# Run tests with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/test_prompt_template.py -v
```

---

## More ScreenShoots

![ScreenShot-001](img/screenshot-001.png)

![ScreenShot-002](img/screenshot-002.png)

![ScreenShot-003](img/screenshot-003.png)

![ScreenShot-004](img/screenshot-004.png)

![ScreenShot-005](img/screenshot-005.png)

![ScreenShot-006](img/screenshot-006.png)

![ScreenShot-007](img/screenshot-007.png)

![ScreenShot-008](img/screenshot-008.png)

![ScreenShot-010](img/screenshot-010.png)


---

## License

This project is licensed under the MIT License.