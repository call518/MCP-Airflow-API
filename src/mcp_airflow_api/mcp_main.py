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

def register_prompts(mcp, api_version: str):
    """Register prompt templates for the MCP server."""
    
    @mcp.prompt()
    async def airflow_cluster_monitoring(dag_name: Optional[str] = None, time_range: Optional[str] = "today") -> str:
        """Comprehensive Airflow cluster monitoring assistant.
        
        Args:
            dag_name: Specific DAG to focus on (optional)
            time_range: Time range for analysis (default: "today")
        """
        
        # Read the prompt template
        import os
        template_path = os.path.join(os.path.dirname(__file__), "prompt_template.md")
        
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                template_content = f.read()
        except FileNotFoundError:
            template_content = "# MCP Airflow API Prompt Template\n\nTemplate file not found."
        
        context = f"""
{template_content}

## Current Session Context

**API Version**: {api_version}
**Target DAG**: {dag_name or "All DAGs"}
**Time Range**: {time_range}
**Available Tools**: {"45 tools (43 common + 2 asset tools)" if api_version == "v2" else "43 tools"}

## Session Instructions

You are an expert Airflow cluster monitoring assistant. Use the available MCP tools to:

1. **Monitor cluster health and performance**
2. **Analyze DAG execution patterns** 
3. **Investigate failures and bottlenecks**
4. **Provide actionable insights**

Always start by checking cluster health with `get_health` and version with `get_version`.
For DAG analysis, use `list_dags`, `running_dags`, and `failed_dags` to get an overview.
"""
        
        if api_version == "v2":
            context += """
## V2 Enhanced Features Available

- **Asset Management**: Use `list_assets` and `list_asset_events` for data-aware scheduling
- **Enhanced DAG Analysis**: All tools support improved filtering and metadata
- **JWT Authentication**: Automatic token management for Airflow 3.0+
"""
        
        return context
    
    @mcp.prompt()
    async def airflow_troubleshooting(issue_type: Optional[str] = "general", severity: Optional[str] = "medium") -> str:
        """Specialized Airflow troubleshooting assistant.
        
        Args:
            issue_type: Type of issue (failed_tasks, slow_dags, resource_issues, general)
            severity: Issue severity (low, medium, high, critical)
        """
        
        context = f"""
# Airflow Troubleshooting Assistant

**API Version**: {api_version}
**Issue Type**: {issue_type}
**Severity**: {severity}

## Troubleshooting Workflow

### 1. Initial Assessment
- Check cluster health: `get_health`
- Review system status: `get_version`
- Identify failed DAGs: `failed_dags`

### 2. Issue-Specific Analysis

**Failed Tasks**: Use `list_task_instances_all` with state="failed"
**Slow DAGs**: Use `dag_run_duration` and `dag_task_duration`
**Resource Issues**: Check `list_pools` and pool utilization
**Import Errors**: Use `list_import_errors` and `all_dag_import_summary`

### 3. Deep Dive Investigation
- Examine task logs: `get_task_instance_logs`
- Review task details: `get_task_instance_details`
- Check XCom data: `list_xcom_entries`
- Analyze event logs: `list_event_logs`

### 4. Monitoring and Prevention
- Set up regular health checks
- Monitor resource utilization trends
- Review configuration settings
"""
        
        if issue_type == "failed_tasks":
            context += """
## Failed Tasks Investigation

1. `failed_dags` - Get overview of failed DAG runs
2. `list_task_instances_all(state="failed")` - List all failed tasks
3. `get_task_instance_logs(dag_id, dag_run_id, task_id)` - Check error logs
4. `get_task_instance_details(dag_id, dag_run_id, task_id)` - Get task metadata
"""
        
        elif issue_type == "slow_dags":
            context += """
## Performance Analysis

1. `dag_run_duration(dag_id)` - Get runtime statistics
2. `dag_task_duration(dag_id, run_id)` - Identify slow tasks
3. `list_task_instances_all(duration_gte=300)` - Find long-running tasks
4. `list_pools` - Check resource allocation
"""
        
        return context
    
    @mcp.prompt()
    async def airflow_dag_analysis(analysis_type: Optional[str] = "overview", dag_pattern: Optional[str] = None) -> str:
        """DAG analysis and optimization assistant.
        
        Args:
            analysis_type: Type of analysis (overview, performance, dependencies, configuration)
            dag_pattern: Pattern to filter DAGs (optional)
        """
        
        context = f"""
# DAG Analysis Assistant

**API Version**: {api_version}
**Analysis Type**: {analysis_type}
**DAG Pattern**: {dag_pattern or "All DAGs"}

## Analysis Workflows

### Overview Analysis
1. `list_dags` - Get DAG inventory
2. `get_dags_detailed_batch(fetch_all=True)` - Comprehensive DAG details
3. `running_dags` and `failed_dags` - Current status

### Performance Analysis  
1. `dag_run_duration(dag_id)` - Runtime trends
2. `dag_task_duration(dag_id, run_id)` - Task-level performance
3. `list_task_instances_all` - Task execution patterns

### Dependencies Analysis
1. `dag_graph(dag_id)` - Task dependency visualization
2. `list_tasks(dag_id)` - Task configuration details
3. `dag_code(dag_id)` - Source code review
"""
        
        if analysis_type == "performance":
            context += """
## Performance Optimization Focus

- Identify bottleneck tasks with high duration
- Check resource utilization patterns
- Analyze failure rates and retry patterns
- Review scheduling efficiency
"""
        
        if api_version == "v2" and analysis_type == "dependencies":
            context += """
## V2 Enhanced Dependencies (Data-Aware Scheduling)

- `list_assets` - Show data assets and dependencies
- `list_asset_events` - Track data lineage and updates
- Enhanced DAG metadata with asset relationships
"""
        
        return context

def create_mcp_server() -> FastMCP:
    """Create and configure the MCP server with tools based on API version."""
    api_version = get_api_version()
    
    # Runtime authentication is configured in main() before mcp.run().
    mcp_instance = FastMCP("mcp-airflow-api")
    
    logger.info(f"Initializing MCP server for Airflow API {api_version}")
    
    # Register prompt templates
    register_prompts(mcp_instance, api_version)
    
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
