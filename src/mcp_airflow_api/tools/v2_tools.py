"""
Airflow API v2 (3.0+) MCP tools.
Imports common tools and adds v2-specific asset management tools.
Total: 56 registered tools (54 common + 2 v2-exclusive).
"""

from typing import Any, Dict, Optional
from ..functions import airflow_request as airflow_request_v2
from . import common_tools
import logging

logger = logging.getLogger(__name__)

def register_tools(mcp):
    """Register v2 tools: common tools + v2-exclusive asset tools."""
    
    logger.info("Initializing MCP server for Airflow API v2")
    logger.info("Loading Airflow API v2 tools (Airflow 3.0+)")
    
    # Set the global request function to v2
    common_tools.airflow_request = airflow_request_v2
    
    # Register all 54 common tools (includes unsupported v1-only operations)
    common_tools.register_common_tools(mcp)
    
    # Add V2-exclusive tools (2 tools)
    @mcp.tool()
    async def list_assets(limit: int = 20, offset: int = 0,
                         uri_pattern: Optional[str] = None) -> Dict[str, Any]:
        """
        [V2 New] List all assets in the system for data-aware scheduling.
        
        Assets are a key feature in Airflow 3.0 for data-aware scheduling.
        They enable workflows to be triggered by data changes rather than time schedules.
        
        Args:
            limit: Maximum number of assets to return (default: 20)
            offset: Number of assets to skip for pagination (default: 0)
            uri_pattern: Filter assets by URI pattern (optional)
            
        Returns:
            Dict containing assets list, pagination info, and metadata
        """
        params = {'limit': limit, 'offset': offset}
        if uri_pattern:
            params['uri_pattern'] = uri_pattern
            
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        
        resp = await airflow_request_v2("GET", f"/assets?{query_string}")
        resp.raise_for_status()
        data = resp.json()
        
        return {
            "assets": data.get("assets", []),
            "total_entries": data.get("total_entries", 0),
            "limit": limit,
            "offset": offset,
            "api_version": "v2",
            "feature": "assets"
        }

    @mcp.tool()
    async def list_asset_events(limit: int = 20, offset: int = 0,
                               asset_uri: Optional[str] = None,
                               source_dag_id: Optional[str] = None) -> Dict[str, Any]:
        """
        [V2 New] List asset events for data lineage tracking.
        
        Asset events track when assets are created or updated by DAGs.
        This enables data lineage tracking and data-aware scheduling in Airflow 3.0.
        
        Args:
            limit: Maximum number of events to return (default: 20)
            offset: Number of events to skip for pagination (default: 0)
            asset_uri: Filter events by specific asset URI (optional)
            source_dag_id: Filter events by source DAG that produced the event (optional)
            
        Returns:
            Dict containing asset events list, pagination info, and metadata
        """
        params = {'limit': limit, 'offset': offset}
        if asset_uri:
            params['asset_uri'] = asset_uri
        if source_dag_id:
            params['source_dag_id'] = source_dag_id
            
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        
        resp = await airflow_request_v2("GET", f"/assets/events?{query_string}")
        resp.raise_for_status()
        data = resp.json()
        
        return {
            "asset_events": data.get("asset_events", []),
            "total_entries": data.get("total_entries", 0),
            "limit": limit,
            "offset": offset,
            "api_version": "v2",
            "feature": "asset_events"
        }

    logger.info(
        "Registered all Airflow API v2 tools (54 common + 2 asset tools = 56 tools)"
    )
