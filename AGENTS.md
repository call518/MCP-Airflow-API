# AGENTS.md

## Maintain the MCP server instructions

`src/mcp_airflow_api/server_instructions.md` is sent to every client in the
MCP initialize response and enters the model's context as-is.

- Keep only rules that span several tools, such as scope and permissions,
  Airflow API v1/v2 availability, result reporting, async semantics, and
  confirmation. Put tool-specific behavior in the tool's docstring and
  parameter descriptions.
- Keep it under 4,096 characters; `tests/test_server_instructions.py` checks
  the limit. Write short imperative English.
- Every tool name, parameter, and environment variable it mentions must match
  the registered tools and settings. When a change renames or removes them, or
  changes a cross-tool rule, update this file in the same change set.
- Never include credentials, private endpoints, or environment-specific data.
- After changing it, run `uv run pytest` and report the actual result.
