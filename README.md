# MCP-Airflow-API

> Use Claude, OpenWebUI, and other MCP clients to inspect and operate Apache
> Airflow through its REST API.

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/mcp-airflow-api)](https://pypi.org/project/mcp-airflow-api/)
[![Docker Pulls](https://img.shields.io/docker/pulls/call518/mcp-server-airflow-api)](https://hub.docker.com/r/call518/mcp-server-airflow-api)
[![Release workflow](https://github.com/call518/MCP-Airflow-API/actions/workflows/pypi-publish.yml/badge.svg)](https://github.com/call518/MCP-Airflow-API/actions/workflows/pypi-publish.yml)

## What is MCP-Airflow-API?

MCP-Airflow-API turns plain-language requests into Apache Airflow REST API
calls. One server supports Airflow 2.x API v1 and Airflow 3.x API v2, selected
with `AIRFLOW_API_VERSION`.

- Inspect DAGs, DAG runs, task instances, pools, variables, connections, and
  cluster health.
- Trigger, pause, and manage supported Airflow resources using the permissions
  of the configured Airflow account.
- Use local stdio or Streamable HTTP transport.
- Run with Claude Desktop, OpenWebUI, any MCP client, or the optional MCPO
  OpenAPI compatibility proxy.

Example requests:

```text
Show the currently running DAGs.
List failed task instances from the last 24 hours.
Trigger the example_etl DAG with today's processing date.
Show assets updated by data_ingestion.  # Airflow 3.x only
```

## Quick Start

Choose one path. Both require an Airflow account and network access to its API.
For a test cluster, see
[Airflow-Docker-Compose](https://github.com/call518/Airflow-Docker-Compose).

<a id="claude-desktop-integration"></a>

### Option A: Claude Desktop with MCPB

1. Download `mcp-airflow-api-<version>.mcpb` from the latest
   [GitHub Release](https://github.com/call518/MCP-Airflow-API/releases/latest).
2. In Claude Desktop, open **Settings → Extensions → Advanced settings →
   Install Extension…** and select the bundle.
3. Choose API `v1` for Airflow 2.x or `v2` for Airflow 3.x, then enter the API
   base URL, user name, and password.
4. Ask: `Show the health of my Airflow cluster.`

The base URL excludes the version suffix, for example
`http://localhost:8080/api`. See the
[Claude Desktop guide](docs/claude-desktop-guide.md) for `uvx`, source, and
remote alternatives.

### Option B: OpenWebUI with Docker Compose

Requirements: Git, Docker with the Compose plugin, and a model provider whose
model supports tool calling.

```bash
git clone https://github.com/call518/MCP-Airflow-API.git
cd MCP-Airflow-API
cp .env.example .env
cp mcp-config.json.http mcp-config.json.http.local
chmod 600 mcp-config.json.http.local
# Edit .env for your Airflow cluster, then start the stack.
docker compose up -d
docker compose ps
```

The default stack exposes only localhost:

- OpenWebUI: `http://localhost:3002`
- MCP server: `http://localhost:18002/mcp`
- Optional MCPO/OpenAPI proxy: `http://localhost:8002`

In OpenWebUI, add an external tool server of type **MCP (Streamable HTTP)**
with the internal Docker URL `http://mcp-server:8000/mcp`. Do not use the host
URL from inside the Docker network.

OpenWebUI connects directly to MCP; MCPO is optional and exists for OpenAPI
clients. Start without it using:

```bash
docker compose up -d mcp-server open-webui
```

The example enables `reasoning_effort: none` globally to avoid tool-call errors
with some reasoning models. Existing OpenWebUI volumes retain their saved model
settings. See the [OpenWebUI guide](docs/openwebui-guide.md) for exact menus,
MCPO setup, authentication, and troubleshooting.

### Other installation methods

| Method | Command or guide |
|---|---|
| PyPI with `uvx` | `uvx --python 3.12 mcp-airflow-api` |
| Claude Desktop JSON configuration | [Claude Desktop guide](docs/claude-desktop-guide.md) |
| Local development | `uv sync --frozen && uv run mcp-airflow-api` |
| Streamable HTTP | `uv run mcp-airflow-api --type streamable-http` |
| OpenWebUI and MCPO | [OpenWebUI guide](docs/openwebui-guide.md) |

## Configuration

| Variable | Required | Description |
|---|---:|---|
| `AIRFLOW_API_VERSION` | No | `v1` for Airflow 2.x (default), `v2` for Airflow 3.x |
| `AIRFLOW_API_BASE_URL` | Yes | API root without `/v1` or `/v2`, such as `http://localhost:8080/api` |
| `AIRFLOW_API_USERNAME` | Yes | Airflow API user name |
| `AIRFLOW_API_PASSWORD` | Yes | Airflow API password |
| `MCP_LOG_LEVEL` | No | `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` |
| `FASTMCP_TYPE` | No | `stdio` (default) or `streamable-http` |
| `FASTMCP_HOST` | HTTP only | Bind address; defaults to `127.0.0.1` |
| `FASTMCP_PORT` | HTTP only | Listen port; defaults to `8000` |
| `REMOTE_AUTH_ENABLE` | HTTP only | Enable Bearer authentication |
| `REMOTE_SECRET_KEY` | When auth enabled | Bearer token shared with MCP clients |

See [Configuration and security](docs/configuration.md) for Docker port
settings, authentication, and client examples.

## Safety and security

- The server can change Airflow state when the configured account permits it.
  Review tool calls and use a least-privilege Airflow account.
- Keep `.env` and `mcp-config.json.*.local` files out of Git. Tracked examples
  contain placeholders only.
- Compose binds HTTP ports to `127.0.0.1` by default. Before exposing the MCP
  server remotely, enable Bearer authentication and put it behind HTTPS.
- MCPB runs locally over stdio and does not use the HTTP Bearer settings.

## Compatibility

- Python 3.12 or newer
- Airflow 2.x through REST API v1
- Airflow 3.x through REST API v2; asset tools are v2-only
- Claude Desktop MCPB, generic stdio/Streamable HTTP MCP clients, OpenWebUI,
  and optional MCPO/OpenAPI clients

Exact endpoint availability still depends on the Airflow version and the
configured account's permissions.

## Documentation

| Guide | Contents |
|---|---|
| [Claude Desktop](docs/claude-desktop-guide.md) | MCPB, `uvx`, source, and remote connections |
| [OpenWebUI](docs/openwebui-guide.md) | Direct MCP, optional MCPO, reasoning models, troubleshooting |
| [Configuration and security](docs/configuration.md) | Environment variables, ports, Bearer authentication |
| [API coverage](api_coverage_analysis.md) | Detailed tool and Airflow endpoint coverage |
| [Prompt and tool catalog](src/mcp_airflow_api/prompt_template.md) | Registered tools and example requests |

## Development

```bash
uv sync --frozen
uv run pytest -ra
```

Run locally over stdio:

```bash
AIRFLOW_API_VERSION=v1 \
AIRFLOW_API_BASE_URL=http://localhost:8080/api \
AIRFLOW_API_USERNAME=airflow \
AIRFLOW_API_PASSWORD=airflow \
uv run mcp-airflow-api
```

Build an MCPB without including local configuration or credentials:

```bash
scripts/build-mcpb.sh 0.0.0 /tmp/mcp-airflow-api-mcpb-check
unzip -l /tmp/mcp-airflow-api-mcpb-check/mcp-airflow-api-0.0.0.mcpb
```

## License

[MIT](LICENSE)
