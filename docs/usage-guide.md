# Usage guide

This guide lists example requests for MCP-Airflow-API. Type them in any
connected MCP client, such as Claude Desktop or OpenWebUI. Tool names before
each request show which tool the model is expected to call; you do not need to
name the tool yourself.

## Example queries by area

Examples marked "API v2 Only" or "v1 API only" work only when
`AIRFLOW_API_VERSION` matches. Replace DAG, task, and run identifiers with
values from your own cluster.

### Basic DAG Operations
- **list_dags**: "List all DAGs with limit 10 in a table format." → Returns up to 10 DAGs
- **list_dags**: "List all DAGs a table format." → Returns up to All DAGs (WARN: Need High Tokens)
- **list_dags**: "Show next page of DAGs." → Use offset for pagination
- **list_dags**: "List DAGs 21-40." → `list_dags(limit=20, offset=20)`
- **list_dags**: "Filter DAGs whose ID contains 'tutorial'." → `list_dags(id_contains="etl")`
- **list_dags**: "Filter DAGs whose display name contains 'tutorial'." → `list_dags(name_contains="daily")`
- **get_dags_detailed_batch**: "Get detailed information for all DAGs with execution status." → `get_dags_detailed_batch(fetch_all=True)`
- **get_dags_detailed_batch**: "Get details for active, unpaused DAGs with recent runs." → `get_dags_detailed_batch(is_active=True, is_paused=False)`
- **get_dags_detailed_batch**: "Get detailed info for DAGs containing 'example' with run history." → `get_dags_detailed_batch(id_contains="example", limit=50)`
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

### Configuration Management
- **get_config**: "Show all Airflow configuration sections and options." → Returns complete config or 403 if expose_config=False
- **list_config_sections**: "List all configuration sections with summary information."
- **get_config_section**: "Get all settings in 'core' section." → `get_config_section("core")`
- **get_config_section**: "Show webserver configuration options." → `get_config_section("webserver")`
- **search_config_options**: "Find all database-related configuration options." → `search_config_options("database")`
- **search_config_options**: "Search for timeout settings in configuration." → `search_config_options("timeout")`

**Important**: Configuration tools require `expose_config = True` in airflow.cfg `[webserver]` section. Even admin users get 403 errors if this is disabled.

### DAG Analysis & Monitoring
- **get_dag**: "Get details for DAG 'example_complex'."
- **get_dags_detailed_batch**: "Get comprehensive details for all DAGs with execution history." → `get_dags_detailed_batch(fetch_all=True)`
- **get_dags_detailed_batch**: "Get details for active DAGs with latest run information." → `get_dags_detailed_batch(is_active=True)`
- **get_dags_detailed_batch**: "Get detailed info for ETL DAGs with recent execution data." → `get_dags_detailed_batch(id_contains="etl")`

**Note**: `get_dags_detailed_batch` returns each DAG with both configuration details (from `get_dag()`) and a `latest_dag_run` field containing the most recent execution information (run_id, state, execution_date, start_date, end_date, etc.).

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

### Asset Management (API v2 Only)
```
# List all assets
list_assets(limit=50, uri_pattern="s3://")

# List asset events for a specific DAG
list_asset_events(source_dag_id="data_pipeline", limit=30)
```

### User & Permissions Management (v1 API only)
- `list_users(limit=20, offset=0)`: List all users in the Airflow system (Airflow 2.x only).
- `get_user(username)`: Get detailed information about a specific user (Airflow 2.x only).
- `list_permissions()`: List all permissions available in the Airflow system (Airflow 2.x only).
- `list_roles(limit=20, offset=0)`: List all roles in the Airflow system (Airflow 2.x only).

### Plugin & Provider Management (Both APIs)
- `list_plugins()`: List all installed plugins in the Airflow system.
- `list_providers()`: List all provider packages installed in the Airflow system.
- `get_provider(provider_name)`: Get detailed information about a specific provider package.

### Dataset Management (v1 API only - use Assets for v2)
- `list_datasets(limit=20, offset=0, uri_pattern=None)`: List all datasets in the system (Airflow 2.x only).
- `get_dataset(dataset_uri)`: Get detailed information about a specific dataset (Airflow 2.x only).
- `list_dataset_events(limit=20, offset=0, dataset_uri=None, source_dag_id=None)`: List dataset events for data lineage tracking (Airflow 2.x only).
- `get_dataset_events(dataset_uri, limit=20, offset=0)`: Get events for a specific dataset (Airflow 2.x only).

**Note**: For Airflow 3.x (v2 API), use `list_assets()` and `list_asset_events()` instead of dataset functions.

### Combined Monitoring
```
# Check cluster health and recent failures
get_health()
failed_dags()

# Monitor specific DAG performance
dag_run_duration("my_dag", limit=20)
dag_task_duration("my_dag", "latest_run_id")

# Check user and permission setup
list_users()
list_roles()
list_permissions()

# Monitor provider and plugin status
list_providers()
list_plugins()

# Track data lineage with datasets
list_datasets()
list_dataset_events()
```
