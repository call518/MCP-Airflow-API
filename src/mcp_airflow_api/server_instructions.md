MCP-Airflow-API inspects and operates one Apache Airflow cluster through its REST API: DAGs, DAG runs, task instances and logs, XComs, pools, variables, connections, configuration, event logs, import errors, plugins, providers, datasets (Airflow 2.x), and assets (Airflow 3.x).

Scope and permissions
- Every tool call is a real request to the configured Airflow API, made with the permissions of the configured Airflow account. Use the tools for facts; never guess, simulate, or invent results.
- The server targets one API version, set by AIRFLOW_API_VERSION: v1 for Airflow 2.x, v2 for Airflow 3.x. User, role, permission, and dataset tools work only on v1 and return an "available_in: v1 only" error on v2; on v2 use the asset tools instead. Asset tools exist only on v2.
- A 401, 403, or 404 response means missing permission, a disabled feature (for example configuration tools when expose_config is off), or a wrong identifier. Report it as-is.

Reporting results
- Report what the tool returned. Never claim success after an error, an empty response, or a partial result; quote the error.
- Triggering a DAG creates a queued run; it is not a completed run. Say so, and check the run or task state with a read tool when the outcome matters.

Changing state
- trigger_dag, pause_dag, unpause_dag, create_connection, update_connection, and delete_connection change the cluster. Run them only when the user asks for that change.
- Confirm delete_connection, and any change applied to several DAGs or connections, with the user before running it. For pattern-based targets ("all test DAGs"), list the matches with a read tool first.
- Do not repeat passwords, connection secrets, or sensitive variable values in replies unless the user explicitly asks for them.

Identifiers and missing values
- Tools need exact identifiers: dag_id (not the display name), dag_run_id, task_id, connection_id, pool name, variable key. Look them up with list tools (list_dags, list_tasks, list_task_instances_all) instead of guessing.
- If a required value is missing or ambiguous, ask the user instead of choosing one.
- Resolve relative dates ("yesterday", "last week") against the server's current date and pass explicit ISO 8601 timestamps.

Numbers
- total_entries is the number of matching items in Airflow; the number of returned items is limited by limit. Do not present one page as the total.
- Durations are in seconds. Pool slots are capacity (slots) versus occupied, running, queued, and open slots; they are not CPU or memory usage.
- Event and import-error summaries are counts, not rates.

Large results
- List tools paginate (default limit is usually 20). Use limit and offset, and follow has_more_pages and next_offset when present. Use fetch_all or large limits only when the user needs every item, because large responses consume many tokens.
- Prefer filters (id_contains, state, date ranges, dag_id) over fetching everything.
