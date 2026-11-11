# Claude Desktop guide

Claude Desktop can run MCP-Airflow-API locally over stdio or connect to a
separately deployed Streamable HTTP server. The MCPB extension is the simplest
local setup.

## MCPB extension

1. Download `mcp-airflow-api-<version>.mcpb` from the latest GitHub Release.
2. Open **Settings → Extensions → Advanced settings → Install Extension…**.
3. Select the downloaded bundle.
4. Enter:
   - `v1` for Airflow 2.x or `v2` for Airflow 3.x.
   - The API base URL without the version suffix, such as
     `http://localhost:8080/api`.
   - An Airflow user name and password.
5. Ask a read-only question such as `Show the health of my Airflow cluster.`

The password field is marked sensitive. The bundle runs locally over stdio and
does not require `REMOTE_AUTH_ENABLE` or `REMOTE_SECRET_KEY`.

## PyPI with uvx

Add this server to the Claude Desktop MCP configuration:

```json
{
  "mcpServers": {
    "mcp-airflow-api": {
      "command": "uvx",
      "args": ["--python", "3.12", "mcp-airflow-api"],
      "env": {
        "AIRFLOW_API_VERSION": "v1",
        "AIRFLOW_API_BASE_URL": "http://localhost:8080/api",
        "AIRFLOW_API_USERNAME": "airflow",
        "AIRFLOW_API_PASSWORD": "replace_with_your_password"
      }
    }
  }
}
```

Restart Claude Desktop after changing its configuration.

## Source checkout

```json
{
  "mcpServers": {
    "mcp-airflow-api": {
      "command": "uv",
      "args": [
        "run",
        "--directory",
        "/absolute/path/to/MCP-Airflow-API",
        "mcp-airflow-api"
      ],
      "env": {
        "AIRFLOW_API_VERSION": "v2",
        "AIRFLOW_API_BASE_URL": "http://localhost:8080/api",
        "AIRFLOW_API_USERNAME": "airflow",
        "AIRFLOW_API_PASSWORD": "replace_with_your_password"
      }
    }
  }
}
```

Use an absolute path. Run `uv sync --frozen` in the checkout first.

## Remote Streamable HTTP

When the server is reachable remotely, configure a connector with:

```text
http://your-server.example:8000/mcp
```

If `REMOTE_AUTH_ENABLE=true`, send:

```text
Authorization: Bearer <REMOTE_SECRET_KEY>
```

Use HTTPS before sending credentials or a Bearer token outside a trusted local
network. The exact connector UI depends on the Claude Desktop version.

## Multiple Airflow clusters

Run one stdio entry or one HTTP server per cluster. Give each entry a distinct
name and its own `AIRFLOW_API_VERSION`, URL, and credentials. A running server
selects one API version at startup.

## Troubleshooting

- Connection refused: confirm the Airflow URL is reachable from the computer
  running Claude Desktop, not only from another container.
- 401 or 403: verify the Airflow credentials and account permissions.
- Endpoint not found: verify `AIRFLOW_API_VERSION` matches the Airflow major
  version and that the base URL does not already end in `/v1` or `/v2`.
- Bundle startup failure: confirm `uv` can install Python 3.12 and the locked
  dependencies on the host.
