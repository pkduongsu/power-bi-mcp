"""
PowerBI MCP Server

A Model Context Protocol server for interacting with PowerBI REST API.
Provides tools for data cleaning, transformation, analysis, and visualization.
"""

import os
import json
import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from dataclasses import dataclass

import httpx
import pandas as pd
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP, Context
from mcp.server.fastmcp.prompts import base

# Load environment variables
load_dotenv()


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class PowerBIContext:
    """Application context for PowerBI operations"""
    access_token: Optional[str] = None
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    base_url: str = "https://api.powerbi.com/v1.0/myorg"
    token_expires: Optional[datetime] = None


class PowerBIClient:
    """PowerBI REST API client"""
    
    def __init__(self, context: PowerBIContext):
        self.context = context
        self.http_client = httpx.AsyncClient()
    
    async def authenticate(self) -> bool:
        """Authenticate with PowerBI using client credentials"""
        if not all([self.context.tenant_id, self.context.client_id, self.context.client_secret]):
            logger.error("Missing authentication credentials")
            return False
        
        # Check if token is still valid
        if (self.context.access_token and self.context.token_expires and 
            datetime.now() < self.context.token_expires):
            return True
        
        auth_url = f"https://login.microsoftonline.com/{self.context.tenant_id}/oauth2/v2.0/token"
        
        data = {
            "client_id": self.context.client_id,
            "client_secret": self.context.client_secret,
            "scope": "https://analysis.windows.net/powerbi/api/.default",
            "grant_type": "client_credentials"
        }
        
        try:
            response = await self.http_client.post(auth_url, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.context.access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", 3600)
            self.context.token_expires = datetime.now() + timedelta(seconds=expires_in - 60)
            
            logger.info("Successfully authenticated with PowerBI")
            return True
            
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False
    
    async def _make_request(self, method: str, endpoint: str, **kwargs) -> httpx.Response:
        """Make authenticated request to PowerBI API"""
        if not await self.authenticate():
            raise Exception("Failed to authenticate with PowerBI")
        
        headers = kwargs.get("headers", {})
        headers["Authorization"] = f"Bearer {self.context.access_token}"
        headers["Content-Type"] = "application/json"
        kwargs["headers"] = headers
        
        url = f"{self.context.base_url}{endpoint}"
        response = await self.http_client.request(method, url, **kwargs)
        response.raise_for_status()
        return response


@asynccontextmanager
async def powerbi_lifespan(server: FastMCP) -> AsyncIterator[PowerBIContext]:
    """Manage PowerBI server lifecycle"""
    # Initialize PowerBI context
    context = PowerBIContext(
        tenant_id=os.getenv("POWERBI_TENANT_ID"),
        client_id=os.getenv("POWERBI_CLIENT_ID"),
        client_secret=os.getenv("POWERBI_CLIENT_SECRET")
    )
    
    # Create PowerBI client
    client = PowerBIClient(context)
    context.client = client
    
    try:
        logger.info("PowerBI MCP Server started")
        yield context
    finally:
        await client.http_client.aclose()
        logger.info("PowerBI MCP Server stopped")


# Create MCP server with lifespan
mcp = FastMCP(
    "PowerBI Server",
    lifespan=powerbi_lifespan,
    dependencies=["httpx", "pandas", "openpyxl"]
)


# Resources
@mcp.resource("workspaces://list")
def list_workspaces() -> str:
    """List all PowerBI workspaces"""
    return "Available PowerBI workspaces in your organization"


@mcp.resource("workspace://{workspace_id}/datasets")
def list_datasets(workspace_id: str) -> str:
    """List datasets in a specific workspace"""
    return f"Datasets in workspace {workspace_id}"


@mcp.resource("workspace://{workspace_id}/reports")
def list_reports(workspace_id: str) -> str:
    """List reports in a specific workspace"""
    return f"Reports in workspace {workspace_id}"


@mcp.resource("dataset://{dataset_id}")
def get_dataset_resource(dataset_id: str) -> str:
    """Get detailed information about a specific dataset"""
    return f"Dataset information for {dataset_id}"


@mcp.resource("dataset://{dataset_id}/schema")
def get_dataset_schema(dataset_id: str) -> str:
    """Get schema information for a dataset"""
    return f"Schema information for dataset {dataset_id}"


# Tools for PowerBI Operations
@mcp.tool()
async def get_workspaces(ctx: Context) -> Dict[str, Any]:
    """Get list of PowerBI workspaces"""
    client = ctx.request_context.lifespan_context.client
    
    try:
        response = await client._make_request("GET", "/groups")
        workspaces = response.json()
        
        return {
            "success": True,
            "workspaces": workspaces.get("value", []),
            "count": len(workspaces.get("value", []))
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
async def get_datasets(ctx: Context, workspace_id: Optional[str] = None) -> Dict[str, Any]:
    """Get datasets from a workspace or all accessible datasets"""
    client = ctx.request_context.lifespan_context.client
    
    try:
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets"
        else:
            endpoint = "/datasets"
        
        response = await client._make_request("GET", endpoint)
        datasets = response.json()
        
        return {
            "success": True,
            "datasets": datasets.get("value", []),
            "workspace_id": workspace_id,
            "count": len(datasets.get("value", []))
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
async def get_dataset(ctx: Context, dataset_id: str, workspace_id: Optional[str] = None) -> Dict[str, Any]:
    """Get detailed information about a specific dataset"""
    client = ctx.request_context.lifespan_context.client
    
    try:
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets/{dataset_id}"
        else:
            endpoint = f"/datasets/{dataset_id}"
        
        response = await client._make_request("GET", endpoint)
        dataset = response.json()
        
        return {
            "success": True,
            "dataset": dataset,
            "dataset_id": dataset_id,
            "workspace_id": workspace_id
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
async def get_dataset_schema(ctx: Context, dataset_id: str, workspace_id: Optional[str] = None) -> Dict[str, Any]:
    """Get schema information for a dataset"""
    client = ctx.request_context.lifespan_context.client
    
    try:
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets/{dataset_id}"
        else:
            endpoint = f"/datasets/{dataset_id}"
        
        response = await client._make_request("GET", endpoint)
        dataset_info = response.json()
        
        # Get tables information
        if workspace_id:
            tables_endpoint = f"/groups/{workspace_id}/datasets/{dataset_id}/tables"
        else:
            tables_endpoint = f"/datasets/{dataset_id}/tables"
        
        tables_response = await client._make_request("GET", tables_endpoint)
        tables = tables_response.json()
        
        return {
            "success": True,
            "dataset_info": dataset_info,
            "tables": tables.get("value", []),
            "dataset_id": dataset_id
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
async def query_dataset(ctx: Context, dataset_id: str, dax_query: str, workspace_id: Optional[str] = None) -> Dict[str, Any]:
    """Execute DAX query against a dataset"""
    client = ctx.request_context.lifespan_context.client
    
    try:
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        else:
            endpoint = f"/datasets/{dataset_id}/executeQueries"
        
        query_data = {
            "queries": [
                {
                    "query": dax_query
                }
            ],
            "serializerSettings": {
                "includeNulls": True
            }
        }
        
        response = await client._make_request("POST", endpoint, json=query_data)
        result = response.json()
        
        return {
            "success": True,
            "query": dax_query,
            "result": result,
            "dataset_id": dataset_id
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
async def refresh_dataset(ctx: Context, dataset_id: str, workspace_id: Optional[str] = None) -> Dict[str, Any]:
    """Refresh a PowerBI dataset"""
    client = ctx.request_context.lifespan_context.client
    
    try:
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
        else:
            endpoint = f"/datasets/{dataset_id}/refreshes"
        
        # Start refresh
        response = await client._make_request("POST", endpoint, json={})
        
        return {
            "success": True,
            "message": "Dataset refresh initiated",
            "dataset_id": dataset_id,
            "status_code": response.status_code
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
async def get_refresh_history(ctx: Context, dataset_id: str, workspace_id: Optional[str] = None, top: int = 5) -> Dict[str, Any]:
    """Get refresh history for a dataset"""
    client = ctx.request_context.lifespan_context.client
    
    try:
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
        else:
            endpoint = f"/datasets/{dataset_id}/refreshes"
        
        params = {"$top": top}
        response = await client._make_request("GET", endpoint, params=params)
        refresh_history = response.json()
        
        return {
            "success": True,
            "refresh_history": refresh_history.get("value", []),
            "dataset_id": dataset_id,
            "count": len(refresh_history.get("value", []))
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
async def create_report(ctx: Context, workspace_id: str, dataset_id: str, report_name: str) -> Dict[str, Any]:
    """Create a new PowerBI report"""
    client = ctx.request_context.lifespan_context.client
    
    try:
        endpoint = f"/groups/{workspace_id}/reports"
        
        report_data = {
            "name": report_name,
            "datasetId": dataset_id
        }
        
        response = await client._make_request("POST", endpoint, json=report_data)
        report = response.json()
        
        return {
            "success": True,
            "report": report,
            "report_id": report.get("id"),
            "workspace_id": workspace_id
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
async def get_reports(ctx: Context, workspace_id: Optional[str] = None) -> Dict[str, Any]:
    """Get PowerBI reports from a workspace or all accessible reports"""
    client = ctx.request_context.lifespan_context.client
    
    try:
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/reports"
        else:
            endpoint = "/reports"
        
        response = await client._make_request("GET", endpoint)
        reports = response.json()
        
        return {
            "success": True,
            "reports": reports.get("value", []),
            "workspace_id": workspace_id,
            "count": len(reports.get("value", []))
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
async def analyze_data_quality(ctx: Context, dataset_id: str, workspace_id: Optional[str] = None) -> Dict[str, Any]:
    """Analyze data quality of a dataset using basic DAX queries"""
    client = ctx.request_context.lifespan_context.client
    
    try:
        # Get dataset schema first
        schema_result = await get_dataset_schema(ctx, dataset_id, workspace_id)
        if not schema_result["success"]:
            return schema_result
        
        tables = schema_result["tables"]
        quality_analysis = {}
        
        for table in tables:
            table_name = table["name"]
            
            # Row count query
            row_count_query = f"EVALUATE ROW(\"RowCount\", COUNTROWS('{table_name}'))"
            
            try:
                count_result = await query_dataset(ctx, dataset_id, row_count_query, workspace_id)
                if count_result["success"]:
                    quality_analysis[table_name] = {
                        "row_count": count_result["result"]["results"][0]["tables"][0]["rows"][0]["[RowCount]"],
                        "columns": len(table.get("columns", [])),
                        "measures": len(table.get("measures", []))
                    }
                else:
                    quality_analysis[table_name] = {"error": "Could not analyze table"}
            except:
                quality_analysis[table_name] = {"error": "Query execution failed"}
        
        return {
            "success": True,
            "dataset_id": dataset_id,
            "quality_analysis": quality_analysis,
            "total_tables": len(tables)
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
async def export_data_to_csv(ctx: Context, dataset_id: str, table_name: str, workspace_id: Optional[str] = None, max_rows: int = 1000) -> Dict[str, Any]:
    """Export data from a PowerBI table to CSV format"""
    client = ctx.request_context.lifespan_context.client
    
    try:
        # Create DAX query to get table data
        dax_query = f"EVALUATE TOPN({max_rows}, '{table_name}')"
        
        # Execute query
        query_result = await query_dataset(ctx, dataset_id, dax_query, workspace_id)
        if not query_result["success"]:
            return query_result
        
        # Extract data from result
        result_data = query_result["result"]["results"][0]["tables"][0]
        rows = result_data["rows"]
        
        # Convert to CSV-like format
        if rows:
            # Get column names from first row keys
            columns = list(rows[0].keys())
            
            # Create CSV content
            csv_lines = [",".join(columns)]
            for row in rows:
                values = [str(row.get(col, "")).replace(",", ";") for col in columns]
                csv_lines.append(",".join(values))
            
            csv_content = "\n".join(csv_lines)
            
            return {
                "success": True,
                "csv_content": csv_content,
                "row_count": len(rows),
                "column_count": len(columns),
                "table_name": table_name
            }
        else:
            return {
                "success": True,
                "csv_content": "",
                "row_count": 0,
                "message": "No data found in table"
            }
    except Exception as e:
        return {"success": False, "error": str(e)}


# Data Transformation Tools
@mcp.tool()
async def create_calculated_column(ctx: Context, dataset_id: str, table_name: str, column_name: str, dax_expression: str, workspace_id: Optional[str] = None) -> Dict[str, Any]:
    """Create a calculated column in a PowerBI table"""
    client = ctx.request_context.lifespan_context.client
    
    try:
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets/{dataset_id}/tables/{table_name}/columns"
        else:
            endpoint = f"/datasets/{dataset_id}/tables/{table_name}/columns"
        
        column_data = {
            "name": column_name,
            "dataType": "String",  # Default to string, can be parameterized
            "expression": dax_expression
        }
        
        response = await client._make_request("POST", endpoint, json=column_data)
        column = response.json()
        
        return {
            "success": True,
            "column": column,
            "table_name": table_name,
            "dataset_id": dataset_id
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
async def create_measure(ctx: Context, dataset_id: str, table_name: str, measure_name: str, dax_expression: str, workspace_id: Optional[str] = None) -> Dict[str, Any]:
    """Create a measure in a PowerBI table"""
    client = ctx.request_context.lifespan_context.client
    
    try:
        if workspace_id:
            endpoint = f"/groups/{workspace_id}/datasets/{dataset_id}/tables/{table_name}/measures"
        else:
            endpoint = f"/datasets/{dataset_id}/tables/{table_name}/measures"
        
        measure_data = {
            "name": measure_name,
            "expression": dax_expression
        }
        
        response = await client._make_request("POST", endpoint, json=measure_data)
        measure = response.json()
        
        return {
            "success": True,
            "measure": measure,
            "table_name": table_name,
            "dataset_id": dataset_id
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# Prompts for PowerBI Operations
@mcp.prompt()
def analyze_dataset_prompt(dataset_name: str) -> List[base.Message]:
    """Generate a comprehensive analysis prompt for a PowerBI dataset"""
    return [
        base.UserMessage(f"Please analyze the PowerBI dataset '{dataset_name}' and provide insights on:"),
        base.UserMessage("1. Data quality and completeness"),
        base.UserMessage("2. Key metrics and trends"),
        base.UserMessage("3. Potential data issues or anomalies"),
        base.UserMessage("4. Recommendations for data improvement"),
        base.AssistantMessage("I'll help you analyze the dataset. Let me start by examining its structure and key characteristics...")
    ]


@mcp.prompt()
def create_dashboard_prompt(business_objective: str) -> List[base.Message]:
    """Generate a dashboard creation prompt based on business objectives"""
    return [
        base.UserMessage(f"I need to create a PowerBI dashboard for: {business_objective}"),
        base.UserMessage("Please help me with:"),
        base.UserMessage("1. Identifying the key metrics to display"),
        base.UserMessage("2. Suggesting appropriate visualizations"),
        base.UserMessage("3. Designing the dashboard layout"),
        base.UserMessage("4. Creating DAX measures if needed"),
        base.AssistantMessage("I'll help you design an effective dashboard. Let's start by understanding your data sources and key performance indicators...")
    ]


@mcp.prompt()
def data_cleaning_prompt(data_issues: str) -> List[base.Message]:
    """Generate a data cleaning and transformation prompt"""
    return [
        base.UserMessage(f"I've identified these data quality issues: {data_issues}"),
        base.UserMessage("Please help me:"),
        base.UserMessage("1. Clean and standardize the data"),
        base.UserMessage("2. Handle missing values appropriately"),
        base.UserMessage("3. Create data transformation rules"),
        base.UserMessage("4. Validate the cleaned data"),
        base.AssistantMessage("I'll help you clean and transform your data. Let's address each issue systematically...")
    ]


@mcp.prompt()
def dax_optimization_prompt(current_measure: str) -> List[base.Message]:
    """Generate a DAX optimization prompt"""
    return [
        base.UserMessage(f"I have this DAX measure that needs optimization: {current_measure}"),
        base.UserMessage("Please help me:"),
        base.UserMessage("1. Identify performance bottlenecks"),
        base.UserMessage("2. Suggest optimized DAX expressions"),
        base.UserMessage("3. Improve readability and maintainability"),
        base.UserMessage("4. Test the optimized version"),
        base.AssistantMessage("I'll analyze your DAX measure and suggest optimizations. Let me review the current expression...")
    ]


@mcp.prompt()
def report_design_prompt(report_type: str, audience: str) -> List[base.Message]:
    """Generate a report design prompt"""
    return [
        base.UserMessage(f"I need to create a {report_type} report for {audience}"),
        base.UserMessage("Please help me with:"),
        base.UserMessage("1. Selecting appropriate visualizations"),
        base.UserMessage("2. Organizing information effectively"),
        base.UserMessage("3. Ensuring the report is actionable"),
        base.UserMessage("4. Making it visually appealing"),
        base.AssistantMessage("I'll help you design an effective report. Let's consider your audience's needs and preferences...")
    ]


if __name__ == "__main__":
    # Run the MCP server
    mcp.run()