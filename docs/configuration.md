# Configuration and security

## Airflow connection

`AIRFLOW_API_BASE_URL` is the API root without the version suffix. The server
combines it with `AIRFLOW_API_VERSION`:

```dotenv
# Airflow 2.x
AIRFLOW_API_VERSION=v1
AIRFLOW_API_BASE_URL=http://localhost:8080/api

# Airflow 3.x
AIRFLOW_API_VERSION=v2
AIRFLOW_API_BASE_URL=http://localhost:8080/api
```

Set `AIRFLOW_API_USERNAME` and `AIRFLOW_API_PASSWORD` to an account with only
the permissions needed by the intended tool calls.

## Transport

stdio is the default and is used by MCPB, `uvx`, and local MCP clients.

```bash
uv run mcp-airflow-api
```

For Streamable HTTP:

```bash
uv run mcp-airflow-api \
  --type streamable-http \
  --host 127.0.0.1 \
  --port 8000
```

The equivalent variables are `FASTMCP_TYPE`, `FASTMCP_HOST`, and
`FASTMCP_PORT`.

## Bearer authentication

Enable authentication before exposing Streamable HTTP outside localhost:

```dotenv
REMOTE_AUTH_ENABLE=true
REMOTE_SECRET_KEY=replace_with_a_strong_random_value
```

Clients must send:

```text
Authorization: Bearer <REMOTE_SECRET_KEY>
```

Bearer authentication does not provide encryption. Use HTTPS through a trusted
reverse proxy for remote access.

## Docker ports

Compose binds to localhost by default:

```dotenv
DOCKER_BIND_ADDRESS=127.0.0.1
DOCKER_EXTERNAL_PORT_OPENWEBUI=3002
DOCKER_EXTERNAL_PORT_MCP_SERVER=18002
DOCKER_EXTERNAL_PORT_MCPO_PROXY=8002
```

Changing `DOCKER_BIND_ADDRESS` to a non-loopback address exposes the service.
Do so only with an appropriate authentication, TLS, and firewall policy.

## Local files

Tracked files are templates. Put deployment-specific values in ignored local
files:

```bash
cp .env.example .env
cp mcp-config.json.http mcp-config.json.http.local
chmod 600 .env mcp-config.json.http.local
```

Compose mounts the MCPO local config read-only. Never put actual credentials in
README files, `.env.example`, tracked JSON, Dockerfiles, or workflows.

## Client URL examples

| Client location | MCP URL |
|---|---|
| Another Compose service | `http://mcp-server:8000/mcp` |
| Same host with default Compose ports | `http://localhost:18002/mcp` |
| Remote client | `https://your-server.example/mcp` |

The MCPO endpoint is separate from the MCP endpoint and is intended for OpenAPI
clients.
