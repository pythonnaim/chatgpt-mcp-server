from fastapi import FastAPI, Request, HTTPException, Header
from pydantic import BaseModel
import psycopg
import os
import json
from dotenv import load_dotenv
from typing import Dict, Optional, List, Any

load_dotenv()  # Load DB creds from .env

app = FastAPI()

from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

AUTH_TOKEN = os.getenv("MCP_AUTH_TOKEN")

# Multiple database configurations from environment variables
DATABASE_CONFIGS = {
    "primary": os.getenv("POSTGRES_URL_PRIMARY") or os.getenv("POSTGRES_URL"),
    "analytics": os.getenv("POSTGRES_URL_ANALYTICS"), 
    "reporting": os.getenv("POSTGRES_URL_REPORTING"),
    "staging": os.getenv("POSTGRES_URL_STAGING"),
}

# Remove None values and keep only configured databases
DATABASE_CONFIGS = {k: v for k, v in DATABASE_CONFIGS.items() if v}

class JsonRpcRequest(BaseModel):
    jsonrpc: str
    method: str
    id: int
    params: dict

def get_database_url(database: str) -> str:
    """Get database URL by name with proper error handling"""
    if database not in DATABASE_CONFIGS:
        available = list(DATABASE_CONFIGS.keys())
        raise ValueError(f"Database '{database}' not found. Available databases: {available}")
    if not DATABASE_CONFIGS[database]:
        raise ValueError(f"Database '{database}' URL is not configured")
    return DATABASE_CONFIGS[database]

def test_database_connection(database: str) -> tuple[bool, str]:
    """Test database connection"""
    try:
        db_url = get_database_url(database)
        with psycopg.connect(db_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                return True, "Connection successful"
    except ValueError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Connection failed: {str(e)}"

@app.post("/mcp")
async def mcp_handler(req: Request, authorization: str = Header(None)):
    # if authorization != f"Bearer {AUTH_TOKEN}":
    #     raise HTTPException(status_code=401, detail="Unauthorized")

    body = await req.json()
    try:
        rpc = JsonRpcRequest(**body)
        method = rpc.method
        params = rpc.params
        query = params.get("query")
        database = params.get("database", "primary")
    except Exception as e:
        return {"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid request"}, "id": None}

    try:
        if method == "fetch_data":
            rows = run_query(query, database)
            return {
                "jsonrpc": "2.0", 
                "id": rpc.id, 
                "result": {
                    "rows": rows,
                    "database": database,
                    "row_count": len(rows)
                }
            }

        elif method == "execute_query":
            row_count = run_exec(query, database)
            return {
                "jsonrpc": "2.0", 
                "id": rpc.id, 
                "result": {
                    "row_count": row_count,
                    "database": database
                }
            }

        elif method == "get_table_names":
            # Get all table names from the database
            database = params.get("database", "primary")
            
            # Test connection first
            connected, message = test_database_connection(database)
            if not connected:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32000, "message": f"Database connection failed: {message}"}, 
                    "id": rpc.id
                }
            
            try:
                table_query = """
                SELECT table_name, table_type, table_schema
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                ORDER BY table_schema, table_name
                """
                tables = run_query(table_query, database)
                
                return {
                    "jsonrpc": "2.0",
                    "id": rpc.id,
                    "result": {
                        "database": database,
                        "tables": tables,
                        "table_count": len(tables)
                    }
                }
            except Exception as e:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32000, "message": f"Failed to fetch tables: {str(e)}"}, 
                    "id": rpc.id
                }

        elif method == "get_table_schema":
            # Get schema for a specific table
            database = params.get("database", "primary")
            table_name = params.get("table_name")
            
            if not table_name:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "table_name parameter is required"}, 
                    "id": rpc.id
                }
            
            try:
                schema_query = """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    ordinal_position
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY ordinal_position
                """
                columns = run_query_with_params(schema_query, [table_name], database)
                
                if not columns:
                    return {
                        "jsonrpc": "2.0", 
                        "error": {"code": -32000, "message": f"Table '{table_name}' not found"}, 
                        "id": rpc.id
                    }
                
                return {
                    "jsonrpc": "2.0",
                    "id": rpc.id,
                    "result": {
                        "database": database,
                        "table_name": table_name,
                        "columns": columns,
                        "column_count": len(columns)
                    }
                }
            except Exception as e:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32000, "message": f"Failed to fetch table schema: {str(e)}"}, 
                    "id": rpc.id
                }

        elif method == "search_across_databases":
            search_term = params.get("search_term")
            databases = params.get("databases") or list(DATABASE_CONFIGS.keys())
            table_pattern = params.get("table_pattern", None)
            
            if not search_term:
                return {
                    "jsonrpc": "2.0", 
                    "error": {"code": -32602, "message": "search_term parameter is required"}, 
                    "id": rpc.id
                }
            
            results = []
            for db in databases:
                try:
                    # Test connection first
                    connected, conn_message = test_database_connection(db)
                    if not connected:
                        results.append({
                            "database": db,
                            "error": f"Connection failed: {conn_message}",
                            "results": []
                        })
                        continue
                    
                    db_results = []
                    
                    if table_pattern:
                        # Search in specific table pattern
                        search_query = """
                        SELECT table_name FROM information_schema.tables 
                        WHERE table_name ILIKE %s 
                        AND table_schema NOT IN ('information_schema', 'pg_catalog')
                        """
                        tables = run_query_with_params(search_query, [f'%{table_pattern}%'], db)
                        
                        for table_row in tables:
                            table_name = table_row['table_name']
                            try:
                                # Get text columns for this table
                                col_query = """
                                SELECT column_name FROM information_schema.columns 
                                WHERE table_name = %s 
                                AND data_type IN ('text', 'varchar', 'character varying', 'char')
                                """
                                columns = run_query_with_params(col_query, [table_name], db)
                                
                                if columns:
                                    col_names = [col['column_name'] for col in columns]
                                    conditions = ' OR '.join([f'"{col}"::text ILIKE %s' for col in col_names])
                                    search_params = [f'%{search_term}%'] * len(col_names)
                                    
                                    data_query = f'SELECT * FROM "{table_name}" WHERE {conditions} LIMIT 10'
                                    table_results = run_query_with_params(data_query, search_params, db)
                                    
                                    if table_results:
                                        db_results.append({
                                            "table": table_name,
                                            "matches": table_results
                                        })
                            except Exception as table_error:
                                continue
                    else:
                        # General search across all text fields
                        tables_query = """
                        SELECT DISTINCT t.table_name
                        FROM information_schema.tables t
                        JOIN information_schema.columns c ON t.table_name = c.table_name
                        WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog')
                        AND c.data_type IN ('text', 'varchar', 'character varying', 'char')
                        LIMIT 20
                        """
                        
                        tables_with_text = run_query(tables_query, db)
                        
                        for table_info in tables_with_text:
                            table_name = table_info['table_name']
                            try:
                                # Get text columns for this table
                                col_query = """
                                SELECT column_name FROM information_schema.columns 
                                WHERE table_name = %s 
                                AND data_type IN ('text', 'varchar', 'character varying', 'char')
                                """
                                columns = run_query_with_params(col_query, [table_name], db)
                                
                                if columns:
                                    col_names = [col['column_name'] for col in columns]
                                    conditions = ' OR '.join([f'"{col}"::text ILIKE %s' for col in col_names])
                                    search_params = [f'%{search_term}%'] * len(col_names)
                                    
                                    data_query = f'SELECT * FROM "{table_name}" WHERE {conditions} LIMIT 5'
                                    table_results = run_query_with_params(data_query, search_params, db)
                                    
                                    if table_results:
                                        db_results.append({
                                            "table": table_name,
                                            "matches": table_results
                                        })
                            except Exception:
                                continue
                    
                    results.append({
                        "database": db,
                        "results": db_results
                    })
                        
                except Exception as e:
                    results.append({
                        "database": db,
                        "error": str(e),
                        "results": []
                    })
            
            return {
                "jsonrpc": "2.0",
                "id": rpc.id,
                "result": {
                    "search_term": search_term,
                    "databases_searched": databases,
                    "results": results
                }
            }

        elif method == "list_databases":
            # Test connections for all databases
            db_status = {}
            for db_name in DATABASE_CONFIGS.keys():
                connected, message = test_database_connection(db_name)
                db_status[db_name] = {
                    "connected": connected,
                    "message": message,
                    "url_configured": bool(DATABASE_CONFIGS[db_name])
                }
            
            return {
                "jsonrpc": "2.0",
                "id": rpc.id,
                "result": {
                    "databases": db_status,
                    "count": len(DATABASE_CONFIGS)
                }
            }

        elif method == "test_connection":
            database = params.get("database", "primary")
            connected, message = test_database_connection(database)
            
            return {
                "jsonrpc": "2.0",
                "id": rpc.id,
                "result": {
                    "database": database,
                    "connected": connected,
                    "message": message
                }
            }

        else:
            available_methods = [
                "fetch_data", "execute_query", "get_table_names", "get_table_schema",
                "search_across_databases", "list_databases", "test_connection"
            ]
            return {
                "jsonrpc": "2.0", 
                "error": {
                    "code": -32601, 
                    "message": f"Method '{method}' not found. Available methods: {available_methods}"
                }, 
                "id": rpc.id
            }

    except Exception as e:
        return {
            "jsonrpc": "2.0", 
            "error": {"code": -32000, "message": str(e)}, 
            "id": rpc.id
        }

def run_query(query: str, database: str = "primary"):
    """Execute a query on the specified database"""
    db_url = get_database_url(database)
    
    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            if cur.description:
                colnames = [desc[0] for desc in cur.description]
                return [dict(zip(colnames, row)) for row in cur.fetchall()]
            else:
                return []

def run_query_with_params(query: str, params: list, database: str = "primary"):
    """Execute a parameterized query on the specified database"""
    db_url = get_database_url(database)
    
    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if cur.description:
                colnames = [desc[0] for desc in cur.description]
                return [dict(zip(colnames, row)) for row in cur.fetchall()]
            else:
                return []

def run_exec(query: str, database: str = "primary"):
    """Execute a command on the specified database"""
    db_url = get_database_url(database)
    
    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.rowcount

# Health check endpoint
@app.get("/health")
async def health_check():
    """Check the health of all configured databases"""
    health_status = {}
    overall_status = "healthy"
    
    if not DATABASE_CONFIGS:
        return {
            "status": "error",
            "message": "No databases configured",
            "databases": {}
        }
    
    for db_name in DATABASE_CONFIGS.keys():
        connected, message = test_database_connection(db_name)
        health_status[db_name] = {
            "connected": connected,
            "message": message
        }
        if not connected:
            overall_status = "degraded"
    
    return {
        "status": overall_status, 
        "databases": health_status,
        "total_databases": len(DATABASE_CONFIGS),
        "configured_databases": list(DATABASE_CONFIGS.keys())
    }

@app.get("/")
async def root():
    """Root endpoint with server info"""
    return {
        "message": "Multi-Database MCP Server",
        "version": "2.0.0",
        "configured_databases": list(DATABASE_CONFIGS.keys()),
        "available_methods": [
            "fetch_data", "execute_query", "get_table_names", "get_table_schema",
            "search_across_databases", "list_databases", "test_connection"
        ],
        "endpoints": {
            "mcp": "/mcp",
            "health": "/health"
        }
    }

if __name__ == "__main__":
    import uvicorn
    print("Starting Multi-Database MCP Server for Deep Search...")
    
    if not DATABASE_CONFIGS:
        print("⚠️  WARNING: No databases configured!")
        print("Add database URLs to your .env file:")
        print("POSTGRES_URL_PRIMARY=postgresql://user:password@host:5432/database")
    else:
        print(f"✅ Configured databases: {list(DATABASE_CONFIGS.keys())}")
        
        # Test connections on startup
        for db_name in DATABASE_CONFIGS.keys():
            connected, message = test_database_connection(db_name)
            status = "✅" if connected else "❌"
            print(f"{status} {db_name}: {message}")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)
