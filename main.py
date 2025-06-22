#!/usr/bin/env python3
"""
PostgreSQL MCP Server for ChatGPT Integration
Enables ChatGPT to query DigitalOcean PostgreSQL databases via Model Context Protocol
"""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import asyncpg
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
class Config:
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:25060/dbname?sslmode=require")
    AUTH_TOKEN = os.getenv("AUTH_TOKEN", "your-secure-bearer-token")
    MAX_ROWS = int(os.getenv("MAX_ROWS", "1000"))
    QUERY_TIMEOUT = int(os.getenv("QUERY_TIMEOUT", "30"))
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", "8000"))

# Pydantic models for MCP protocol
class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    id: Optional[str] = None
    method: str
    params: Optional[Dict[str, Any]] = None

class MCPResponse(BaseModel):
    jsonrpc: str = "2.0"
    id: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None

class Tool(BaseModel):
    name: str
    description: str
    inputSchema: Dict[str, Any] = Field(alias="input_schema")

class ToolCallRequest(BaseModel):
    name: str
    arguments: Dict[str, Any]

# Database connection pool
class DatabasePool:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
    
    async def initialize(self):
        """Initialize the database connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                Config.DATABASE_URL,
                min_size=1,
                max_size=10,
                command_timeout=Config.QUERY_TIMEOUT
            )
            logger.info("Database pool initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    async def close(self):
        """Close the database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")
    
    @asynccontextmanager
    async def get_connection(self):
        """Get a database connection from the pool"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        
        async with self.pool.acquire() as connection:
            yield connection

# Global database pool instance
db_pool = DatabasePool()

# Security
security = HTTPBearer()

async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify the Bearer token"""
    if credentials.credentials != Config.AUTH_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid authentication token")
    return credentials.credentials

# FastAPI app with lifespan management
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting MCP PostgreSQL Server...")
    await db_pool.initialize()
    yield
    # Shutdown
    logger.info("Shutting down MCP PostgreSQL Server...")
    await db_pool.close()

app = FastAPI(
    title="PostgreSQL MCP Server",
    description="Model Context Protocol server for PostgreSQL database access",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://chatgpt.com", "https://chat.openai.com"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# MCP Tools Definition
TOOLS = [
    Tool(
        name="fetch_data",
        description="Execute a SELECT query to fetch data from the PostgreSQL database. Use this for reading data, analyzing trends, or generating reports.",
        input_schema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The SQL SELECT query to execute. Should be read-only."
                },
                "description": {
                    "type": "string",
                    "description": "A brief description of what this query is meant to accomplish."
                }
            },
            "required": ["query"]
        }
    ),
    Tool(
        name="get_schema",
        description="Get the database schema information including tables, columns, and their types.",
        input_schema={
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Optional: specific table name to get schema for. If not provided, returns all tables."
                }
            }
        }
    ),
    Tool(
        name="execute_query",
        description="Execute any SQL query (SELECT, INSERT, UPDATE, DELETE). Use with caution for write operations.",
        input_schema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string", 
                    "description": "The SQL query to execute."
                },
                "description": {
                    "type": "string",
                    "description": "A brief description of what this query is meant to accomplish."
                }
            },
            "required": ["query"]
        }
    )
]

async def execute_sql_query(query: str, fetch_results: bool = True) -> Dict[str, Any]:
    """Execute SQL query and return results with metadata"""
    start_time = time.time()
    
    try:
        async with db_pool.get_connection() as conn:
            if fetch_results:
                # For SELECT queries
                rows = await conn.fetch(query)
                execution_time = time.time() - start_time
                
                # Convert rows to list of dictionaries
                result_data = []
                if rows:
                    columns = list(rows[0].keys())
                    for row in rows[:Config.MAX_ROWS]:  # Limit rows
                        result_data.append({col: val for col, val in row.items()})
                    
                    if len(rows) > Config.MAX_ROWS:
                        logger.warning(f"Query returned {len(rows)} rows, limited to {Config.MAX_ROWS}")
                else:
                    columns = []
                
                return {
                    "success": True,
                    "data": result_data,
                    "columns": columns,
                    "row_count": len(result_data),
                    "total_rows": len(rows),
                    "execution_time": round(execution_time, 3),
                    "truncated": len(rows) > Config.MAX_ROWS
                }
            else:
                # For non-SELECT queries
                result = await conn.execute(query)
                execution_time = time.time() - start_time
                
                return {
                    "success": True,
                    "result": result,
                    "execution_time": round(execution_time, 3)
                }
                
    except asyncpg.PostgresError as e:
        execution_time = time.time() - start_time
        error_msg = f"Database error: {str(e)}"
        logger.error(f"SQL execution failed: {error_msg}")
        
        return {
            "success": False,
            "error": error_msg,
            "execution_time": round(execution_time, 3)
        }
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"Unexpected error during SQL execution: {error_msg}")
        
        return {
            "success": False,
            "error": error_msg,
            "execution_time": round(execution_time, 3)
        }

async def get_database_schema(table_name: Optional[str] = None) -> Dict[str, Any]:
    """Get database schema information"""
    try:
        if table_name:
            # Get schema for specific table
            query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_name = $1
            ORDER BY ordinal_position;
            """
            async with db_pool.get_connection() as conn:
                rows = await conn.fetch(query, table_name)
        else:
            # Get all tables and their columns
            query = """
            SELECT 
                t.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.character_maximum_length
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c ON t.table_name = c.table_name
            WHERE t.table_schema = 'public' 
            AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name, c.ordinal_position;
            """
            async with db_pool.get_connection() as conn:
                rows = await conn.fetch(query)
        
        # Process results
        if table_name:
            columns = []
            for row in rows:
                columns.append({
                    "column_name": row["column_name"],
                    "data_type": row["data_type"],
                    "is_nullable": row["is_nullable"],
                    "column_default": row["column_default"],
                    "character_maximum_length": row["character_maximum_length"]
                })
            return {
                "success": True,
                "table_name": table_name,
                "columns": columns
            }
        else:
            tables = {}
            for row in rows:
                table_name = row["table_name"]
                if table_name not in tables:
                    tables[table_name] = []
                
                if row["column_name"]:  # Only add if column exists
                    tables[table_name].append({
                        "column_name": row["column_name"],
                        "data_type": row["data_type"],
                        "is_nullable": row["is_nullable"],
                        "column_default": row["column_default"],
                        "character_maximum_length": row["character_maximum_length"]
                    })
            
            return {
                "success": True,
                "tables": tables
            }
            
    except Exception as e:
        logger.error(f"Failed to get database schema: {e}")
        return {
            "success": False,
            "error": str(e)
        }

# MCP Protocol Endpoints

@app.post("/mcp")
async def mcp_handler(request: MCPRequest, token: str = Depends(verify_token)):
    """Main MCP protocol handler"""
    logger.info(f"MCP request: {request.method}")
    
    try:
        if request.method == "initialize":
            return MCPResponse(
                id=request.id,
                result={
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {}
                    },
                    "serverInfo": {
                        "name": "postgresql-mcp-server",
                        "version": "1.0.0"
                    }
                }
            )
        
        elif request.method == "tools/list":
            return MCPResponse(
                id=request.id,
                result={
                    "tools": [tool.dict(by_alias=True) for tool in TOOLS]
                }
            )
        
        elif request.method == "tools/call":
            if not request.params:
                raise HTTPException(status_code=400, detail="Missing parameters for tool call")
            
            tool_name = request.params.get("name")
            arguments = request.params.get("arguments", {})
            
            result = await handle_tool_call(tool_name, arguments)
            
            return MCPResponse(
                id=request.id,
                result={
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(result, indent=2, default=str)
                        }
                    ]
                }
            )
        
        else:
            return MCPResponse(
                id=request.id,
                error={
                    "code": -32601,
                    "message": f"Method not found: {request.method}"
                }
            )
            
    except Exception as e:
        logger.error(f"Error handling MCP request: {e}")
        return MCPResponse(
            id=request.id,
            error={
                "code": -32603,
                "message": f"Internal error: {str(e)}"
            }
        )

async def handle_tool_call(tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Handle tool calls"""
    logger.info(f"Tool call: {tool_name} with args: {arguments}")
    
    if tool_name == "fetch_data":
        query = arguments.get("query")
        description = arguments.get("description", "Data fetch query")
        
        if not query:
            return {"success": False, "error": "Query parameter is required"}
        
        # Ensure it's a SELECT query for safety
        query_upper = query.strip().upper()
        if not query_upper.startswith("SELECT"):
            return {"success": False, "error": "Only SELECT queries are allowed for fetch_data"}
        
        result = await execute_sql_query(query, fetch_results=True)
        result["description"] = description
        return result
    
    elif tool_name == "get_schema":
        table_name = arguments.get("table_name")
        return await get_database_schema(table_name)
    
    elif tool_name == "execute_query":
        query = arguments.get("query")
        description = arguments.get("description", "SQL query execution")
        
        if not query:
            return {"success": False, "error": "Query parameter is required"}
        
        # Determine if this is a SELECT query
        query_upper = query.strip().upper()
        is_select = query_upper.startswith("SELECT")
        
        result = await execute_sql_query(query, fetch_results=is_select)
        result["description"] = description
        return result
    
    else:
        return {"success": False, "error": f"Unknown tool: {tool_name}"}

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        async with db_pool.get_connection() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Database connection failed")

# Root endpoint with server info
@app.get("/")
async def root():
    """Root endpoint with server information"""
    return {
        "name": "PostgreSQL MCP Server",
        "version": "1.0.0",
        "description": "Model Context Protocol server for PostgreSQL database access",
        "endpoints": {
            "/mcp": "Main MCP protocol endpoint",
            "/health": "Health check endpoint",
            "/": "This information endpoint"
        }
    }

if __name__ == "__main__":
    # Print configuration (without sensitive data)
    logger.info("Starting PostgreSQL MCP Server")
    logger.info(f"Host: {Config.HOST}:{Config.PORT}")
    logger.info(f"Max rows: {Config.MAX_ROWS}")
    logger.info(f"Query timeout: {Config.QUERY_TIMEOUT}s")
    
    # Run the server
    uvicorn.run(
        "main:app",
        host=Config.HOST,
        port=Config.PORT,
        reload=False,
        access_log=True
    )