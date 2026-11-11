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

## Connect a model provider

This step applies to both the direct MCP connection and MCPO. Open
`http://localhost:3002`, create the administrator account, and add one
provider whose model supports reliable native tool calling:

- **OpenAI:** open **Settings → Admin → Connections**, add a connection with
  URL `https://api.openai.com/v1`, **Auth** `Bearer` with your OpenAI API key,
  and **API Type** set to **Chat Completions** for the Quick Start default (see
  [OpenAI API type and reasoning models](#openai-api-type-and-reasoning-models)),
  then save it.
- **Another OpenAI-compatible provider:** add its base URL and API key under
  **Settings → Admin → Connections**. Enter the model ID manually when the
  provider does not expose a model list.

Confirm that the model appears in a new chat before adding the MCP connection.

## Direct MCP connection

1. Under **Settings → Admin → Tools → Integrations → External Tool Servers**,
   add a connection with type **MCP (Streamable HTTP)** and URL
   `http://mcp-server:8000/mcp`.
2. If MCP Bearer authentication is enabled, configure the same Bearer token on
   the connection. Otherwise choose no authentication.
3. Enable the connection for a new chat and ask
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

## OpenAI API type and reasoning models

For OpenAI reasoning models, the connection's **API Type** and the **Reasoning
Effort** parameter must match. Set **API Type** under **Settings → Admin →
Connections → edit the OpenAI connection**, and use one of these combinations:

| Setup | API Type | Reasoning Effort | Result |
|---|---|---|---|
| Quick Start default | **Chat Completions** | `none` (from `DEFAULT_MODEL_PARAMS`) | Tools work; model reasoning is off |
| Reasoning with tools | **Responses** | **Default** in every scope | Tools work; the model uses its default reasoning effort |

Other combinations fail:

- **Responses** with any `reasoning_effort` value, including `none`: OpenWebUI
  sends the Chat Completions parameter `reasoning_effort` unchanged, and the
  Responses API rejects it with `Unsupported parameter: 'reasoning_effort'`.
- **Chat Completions** with a reasoning effort other than `none`: the API
  rejects the request with `Function tools with reasoning_effort are not
  supported`.

`.env.example` sets the Quick Start default:

```dotenv
DEFAULT_MODEL_PARAMS={"reasoning_effort":"none"}
```

OpenWebUI seeds the value into its database only on the first startup.

- Existing `.env`: add the setting manually.
- Existing `open-webui-data` volume: update Model Defaults in the administrator
  UI; do not delete the volume.
- Per-model or per-chat override: set Reasoning Effort to `none`.
- Provider rejects the parameter: remove the global value and configure only
  models that support it.

The usual precedence is global defaults, per-model settings, then per-chat
settings.

To use model reasoning instead, set `DEFAULT_MODEL_PARAMS={}` before the first
start, set the connection's **API Type** to **Responses**, and leave
**Reasoning Effort** at **Default** under **Settings → Admin → Models → Model
Defaults** and in each model's **Advanced Params**. Remove any
`reasoning_effort` override from the chat's **Controls → Advanced Params** and
**Settings → General → Advanced Parameters**, then start a new chat. OpenWebUI
merges parameters from these scopes, so an old override can still make the
request fail.

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

### OpenAI requests fail with `reasoning_effort` errors

| Error | Fix |
|---|---|
| `Unsupported parameter: 'reasoning_effort'` | Set **API Type** to **Chat Completions** (Quick Start default), or keep **Responses** and set Reasoning Effort to **Default** in every scope; then start a new chat |
| `Function tools with reasoning_effort are not supported` | Set Reasoning Effort to `none` (Quick Start default), or switch **API Type** to **Responses** with Reasoning Effort at **Default** |

### OpenWebUI cannot reach MCP

- From OpenWebUI, use `http://mcp-server:8000/mcp`.
- From the host, use `http://localhost:18002/mcp`.
- Wait for `docker compose ps` to report the MCP server healthy.
- Check `docker compose logs mcp-server` without exposing credentials.
