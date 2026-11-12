"""
Dynamic MCP server that loads API version-specific tools based on AIRFLOW_API_VERSION.

- Airflow API v1 Documents: https://airflow.apache.org/docs/apache-airflow/2.0.0/stable-rest-api-ref.html
- Airflow API v2 Documents: https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
"""
import argparse
import asyncio
import logging
from typing import Any, Dict, List, Optional
from fastmcp import FastMCP

# Try to import StaticTokenVerifier, fallback if not available
try:
    from fastmcp.server.auth import StaticTokenVerifier
    HAS_AUTH_SUPPORT = True
except ImportError:
    StaticTokenVerifier = None
    HAS_AUTH_SUPPORT = False

import os
import argparse
import logging

from mcp_airflow_api.functions import get_api_version

# Sent to clients as the MCP server instructions. Claude Code truncates
# instructions longer than 4,096 characters, so keep this file well below that.
SERVER_INSTRUCTIONS_PATH = os.path.join(
    os.path.dirname(__file__), "server_instructions.md"
)


TRUTHY_VALUES = ("true", "1", "yes", "on")

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# =============================================================================
# Authentication Setup
# =============================================================================

def _parse_bool_env(value: str) -> bool:
    return value.strip().lower() in TRUTHY_VALUES


def _build_static_token_auth(secret_key: str) -> Any:
    tokens = {
        secret_key: {
            "client_id": "airflow-api-client",
            "scopes": ["read", "write"],
        }
    }
    return StaticTokenVerifier(tokens=tokens)


def create_mcp_server() -> FastMCP:
    """Create and configure the MCP server with tools based on API version."""
    api_version = get_api_version()
    
    # Runtime authentication is configured in main() before mcp.run().
    with open(SERVER_INSTRUCTIONS_PATH, encoding="utf-8") as f:
        instructions = f.read()
    mcp_instance = FastMCP("mcp-airflow-api", instructions=instructions)
    
    logger.info(f"Initializing MCP server for Airflow API {api_version}")
    
    if api_version == "v1":
        logger.info("Loading Airflow API v1 tools (Airflow 2.x)")
        from mcp_airflow_api.tools import v1_tools
        v1_tools.register_tools(mcp_instance)
    elif api_version == "v2":
        logger.info("Loading Airflow API v2 tools (Airflow 3.0+)")
        from mcp_airflow_api.tools import v2_tools
        v2_tools.register_tools(mcp_instance)
    else:
        raise ValueError(f"Unsupported API version: {api_version}. Use 'v1' or 'v2'")
    
    logger.info(f"MCP server initialized with API version {api_version}")
    return mcp_instance

# Create the MCP server instance
mcp = create_mcp_server()

def main(argv: Optional[List[str]] = None):
    """Entrypoint for MCP Airflow API server.

    Supports optional CLI arguments (e.g. --log-level DEBUG) while remaining
    backward-compatible with stdio launcher expectations.
    """
    parser = argparse.ArgumentParser(prog="mcp-airflow-api", description="MCP Airflow API Server")
    parser.add_argument(
        "--log-level",
        dest="log_level",
        help="Logging level override (DEBUG, INFO, WARNING, ERROR, CRITICAL). Overrides MCP_LOG_LEVEL env if provided.",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    parser.add_argument(
        "--type",
        dest="transport_type",
        help="Transport type. Default: env FASTMCP_TYPE or stdio",
        choices=["stdio", "streamable-http"],
        default=None,
    )
    parser.add_argument(
        "--host",
        dest="host",
        help="Host address for streamable-http transport. Default: 127.0.0.1",
    )
    parser.add_argument(
        "--port",
        dest="port",
        type=int,
        help="Port number for streamable-http transport. Default: 8000",
    )
    auth_group = parser.add_mutually_exclusive_group()
    auth_group.add_argument(
        "--auth-enable",
        dest="auth_enable",
        action="store_true",
        default=None,
        help="Enable Bearer token authentication for streamable-http mode.",
    )
    auth_group.add_argument(
        "--auth-disable",
        dest="auth_enable",
        action="store_false",
        default=None,
        help="Disable Bearer token authentication for streamable-http mode.",
    )
    parser.add_argument(
        "--secret-key",
        dest="secret_key",
        help="Secret key for Bearer token authentication. Required when auth is enabled.",
    )
    # Allow future extension without breaking unknown args usage
    args = parser.parse_args(argv)

    # Determine log level: CLI arg > environment variable > default
    log_level = args.log_level or os.getenv("MCP_LOG_LEVEL", "INFO")
    
    # Set logging level
    logging.getLogger().setLevel(log_level)
    logger.setLevel(log_level)
    logging.getLogger("aiohttp.client").setLevel("WARNING")  # reduce noise at DEBUG
    
    if args.log_level:
        logger.info("Log level set via CLI to %s", args.log_level)
    elif os.getenv("MCP_LOG_LEVEL"):
        logger.info("Log level set via environment variable to %s", log_level)
    else:
        logger.info("Using default log level: %s", log_level)

    # 우선순위: 실행옵션 > 환경변수 > 기본값
    # Transport type 결정
    transport_type = args.transport_type or os.getenv("FASTMCP_TYPE", "stdio")
    
    # Host 결정
    host = args.host or os.getenv("FASTMCP_HOST", "127.0.0.1")
    
    # Port 결정 (간결하게)
    port = args.port or int(os.getenv("FASTMCP_PORT", 8000))
    
    # Authentication 설정 결정
    # REMOTE_AUTH_ENABLE defaults to "false" when undefined, empty, or null
    # Supported values: true/false, 1/0, yes/no, on/off (case insensitive)
    if args.auth_enable is None:
        auth_enable = _parse_bool_env(os.getenv("REMOTE_AUTH_ENABLE", "false"))
    else:
        auth_enable = args.auth_enable
    secret_key = args.secret_key or os.getenv("REMOTE_SECRET_KEY", "")
    
    # Validation for streamable-http mode with authentication
    if transport_type == "streamable-http":
        if auth_enable:
            if not HAS_AUTH_SUPPORT:
                logger.error("ERROR: Bearer token authentication requested but not supported by current fastmcp version")
                logger.error("Please upgrade fastmcp to a version that supports StaticTokenVerifier")
                return
            if not secret_key:
                logger.error("ERROR: Authentication is enabled but no secret key provided.")
                logger.error("Please set REMOTE_SECRET_KEY environment variable or use --secret-key argument.")
                return
            logger.info("Authentication enabled for streamable-http transport")
        else:
            logger.warning("WARNING: streamable-http mode without authentication enabled!")
            logger.warning("This server will accept requests without Bearer token verification.")
            logger.warning("Set REMOTE_AUTH_ENABLE=true and REMOTE_SECRET_KEY to enable authentication.")
    elif auth_enable:
        logger.warning("WARNING: Authentication is only supported in streamable-http mode, ignoring auth settings")
    
    # Configure authentication provider before server startup.
    if auth_enable:
        try:
            mcp.auth = _build_static_token_auth(secret_key)
        except Exception as e:
            logger.error(f"ERROR: Failed to initialize StaticTokenVerifier: {e}")
            return
    else:
        mcp.auth = None
    
    # Transport 모드에 따른 실행
    if transport_type == "streamable-http":
        logger.info(f"Starting streamable-http server on {host}:{port}")
        mcp.run(transport="streamable-http", host=host, port=port)
    else:
        logger.info("Starting stdio transport for local usage")
        mcp.run(transport='stdio')
