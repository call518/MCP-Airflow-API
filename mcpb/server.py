"""MCPB entry point for the MCP-Airflow-API stdio server."""

from mcp_airflow_api.mcp_main import main


if __name__ == "__main__":
    main(["--type", "stdio"])
