Model Context Protocol (MCP) server for Apache Airflow API integration.  
This project provides natural language MCP tools for essential Airflow cluster operations.

[![Deploy to PyPI with tag](https://github.com/call518/MCP-Airflow-API/actions/workflows/pypi-publish.yml/badge.svg)](https://github.com/call518/MCP-Airflow-API/actions/workflows/pypi-publish.yml)

[![smithery badge](https://smithery.ai/badge/@call518/mcp-airflow-api)](https://smithery.ai/server/@call518/mcp-airflow-api)

---


# MCP-Airflow-API

**Tested and supported Airflow version: 2.10.2 (API Version: v1)**

## Features

- List all DAGs in the Airflow cluster
- Monitor running/failed DAG runs
- Trigger DAG runs on demand
- Minimal, LLM-friendly output for all tools
- Easy integration with MCP Inspector, OpenWebUI, Smithery, etc.

---

## Available MCP Tools

### DAG Management

- `list_dags`  
	Returns all DAGs registered in the Airflow cluster.  
	Output: `dag_id`, `dag_display_name`, `is_active`, `is_paused`, `owners`, `tags`

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

### DAG Analysis & Monitoring

- `dag_details(dag_id)`  
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
	Output: `event_logs`, `total_entries`, `limit`, `offset`

- `get_event_log(event_log_id)`  
	Retrieves a specific event log entry by ID.  
	Output: `event_log_id`, `when`, `event`, `dag_id`, `task_id`, `run_id`, etc.

- `all_dag_event_summary()`  
	Retrieves event count summary for all DAGs.  
	Output: `dag_summaries`, `total_dags`, `total_events`

- `list_import_errors(limit=20, offset=0)`  
	Lists import errors with optional filtering.  
	Output: `import_errors`, `total_entries`, `limit`, `offset`

- `get_import_error(import_error_id)`  
	Retrieves a specific import error by ID.  
	Output: `import_error_id`, `filename`, `stacktrace`, `timestamp`

- `all_dag_import_summary()`  
	Retrieves import error summary for all DAGs.  
	Output: `import_summaries`, `total_errors`, `affected_files`

- `dag_run_duration(dag_id, limit=10)`  
	Retrieves run duration statistics for a specific DAG.  
	Output: `dag_id`, `runs`, duration analysis, success/failure stats

- `dag_task_duration(dag_id, run_id=None)`  
	Retrieves task duration information for a specific DAG run.  
	Output: `dag_id`, `run_id`, `tasks`, individual task performance

- `dag_calendar(dag_id, start_date=None, end_date=None)`  
	Retrieves calendar/schedule information for a specific DAG.  
	Output: `dag_id`, `schedule_interval`, `runs`, upcoming executions

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

1. Prepare an Airflow cluster  
	 - See [Official Airflow Docker Install Guide](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)

2. Prepare MCP Tools environment  
	 - Install Docker and Docker Compose
	 - Clone this project and run `docker-compose up -d` in the root directory

3. Register the MCP server in MCP Inspector/Smithery  
	 - Example address: `http://localhost:8000/airflow-api`

---

## Logging & Observability

- Structured logs for all tool invocations and HTTP requests
- Control log level via environment variable (`AIRFLOW_LOG_LEVEL`) or CLI flag (`--log-level`)
- Supported levels: DEBUG, INFO, WARNING, ERROR, CRITICAL

---

## License

This project is licensed under the MIT License.

---

## Roadmap

This project starts with a minimal set of essential Airflow management tools. Many more useful features and tools for Airflow cluster operations will be added soon, including advanced monitoring, DAG/task analytics, scheduling controls, and more. Contributions and suggestions are welcome!

---

## Additional Links

- [Code](https://github.com/call518/MCP-Airflow-API)
- [Issues](https://github.com/call518/MCP-Airflow-API/issues)
- [Smithery Deployment](https://smithery.ai/server/@call518/mcp-airflow-api)


