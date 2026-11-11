# OpenWebUI guide

The Compose stack runs OpenWebUI 0.13.0, MCP-Airflow-API, and the optional MCPO
OpenAPI proxy. OpenWebUI can connect directly to MCP; MCPO is not required for
that path.

## Start the stack

```bash
cp .env.example .env
cp mcp-config.json.http mcp-config.json.http.local
chmod 600 mcp-config.json.http.local
```

Edit `.env` with the target Airflow API version, base URL, and credentials. If
Bearer authentication is enabled, set the same token in `.env` and the
`Authorization` header in `mcp-config.json.http.local`.

Generate a stable OpenWebUI session key:

```bash
openssl rand -hex 32
```

Use its output as `WEBUI_SECRET_KEY`, then start:

```bash
docker compose up -d
docker compose ps
```

The default host URLs are OpenWebUI at `http://localhost:3002`, MCP at
`http://localhost:18002/mcp`, and MCPO at `http://localhost:8002`.

## Direct MCP connection

1. Open `http://localhost:3002` and create the administrator account.
2. Configure a model provider under the administrator connection settings.
3. Add an external tool server with type **MCP (Streamable HTTP)** and URL
   `http://mcp-server:8000/mcp`.
4. If MCP Bearer authentication is enabled, configure the same Bearer token on
   the connection. Otherwise choose no authentication.
5. Enable the connection for a new chat and ask
   `Use mcp-airflow-api to show the Airflow cluster health.`

`http://mcp-server:8000/mcp` is the Docker-network URL. A browser or client
running on the host uses `http://localhost:18002/mcp` instead.

## Optional MCPO connection

MCPO exposes the MCP tools as OpenAPI. It reads the ignored local configuration
selected by `MCPO_CONFIG_FILE` and is available at `http://localhost:8002`.
Use this only for clients that require OpenAPI rather than MCP.

Run without MCPO:

```bash
docker compose up -d mcp-server open-webui
```

## Reasoning model compatibility

`.env.example` sets:

```dotenv
DEFAULT_MODEL_PARAMS={"reasoning_effort":"none"}
```

This avoids environments where Chat Completions rejects function tools together
with reasoning effort. OpenWebUI seeds the value into its database only on the
first startup.

- Existing `.env`: add the setting manually.
- Existing `open-webui-data` volume: update Model Defaults in the administrator
  UI; do not delete the volume.
- Per-model or per-chat override: set Reasoning Effort to `none`.
- Provider rejects the parameter: remove the global value and configure only
  models that support it.

The usual precedence is global defaults, per-model settings, then per-chat
settings.

## Troubleshooting

### MCP or MCPO returns 401

Check, without printing the token itself:

1. `REMOTE_AUTH_ENABLE` in `.env`.
2. Whether `REMOTE_SECRET_KEY` is set.
3. The file selected by `MCPO_CONFIG_FILE`.
4. Whether its `Authorization` header is still a placeholder.
5. Whether both token values match.
6. Whether containers were recreated after the change.

An MCPO failure does not prove the direct OpenWebUI MCP connection is broken,
and a successful direct connection does not prove MCPO works. Test them
separately.

### OpenWebUI cannot reach MCP

- From OpenWebUI, use `http://mcp-server:8000/mcp`.
- From the host, use `http://localhost:18002/mcp`.
- Wait for `docker compose ps` to report the MCP server healthy.
- Check `docker compose logs mcp-server` without exposing credentials.
