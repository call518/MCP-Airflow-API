"""
Airflow API v1 (2.x) MCP tools.
Imports common tools and sets v1-specific request function.
Total: 54 tools, all from common_tools.
"""

from ..functions import airflow_request as airflow_request_v1
from . import common_tools
import logging

logger = logging.getLogger(__name__)

def register_tools(mcp):
    """Register v1 tools by importing common tools with v1 request function."""
    
    logger.info("Initializing MCP server for Airflow API v1")
    logger.info("Loading Airflow API v1 tools (Airflow 2.x)")
    
    # Set the global request function to v1
    common_tools.airflow_request = airflow_request_v1
    
    # Register all 54 common tools (includes management tools)
    common_tools.register_common_tools(mcp)
    
    # V1 has no exclusive tools - all tools are shared with v2
    
    logger.info("Registered all Airflow API v1 tools (54 common tools)")
