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
    allow_origins=["*"],  # or specific domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

AUTH_TOKEN = os.getenv("MCP_AUTH_TOKEN")

# Multiple database configurations from environment variables
DATABASE_CONFIGS = {
    "primary": os.getenv("POSTGRES_URL_PRIMARY") or os.getenv("POSTGRES_URL"),  # Fallback to original
    "analytics": os.getenv("POSTGRES_URL_ANALYTICS"), 
    "reporting": os.getenv("POSTGRES_URL_REPORTING"),
    "staging": os.getenv("POSTGRES_URL_STAGING"),
}

# Remove None values
DATABASE_CONFIGS = {k: v for k, v in DATABASE_CONFIGS.items() if v}

class JsonRpcRequest(BaseModel):
    jsonrpc: str
    method: str
    id: int
    params: dict

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
        database = params.get("database", "primary")  # Default to primary database
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

        elif method == "search_across_databases":
            # Deep search across multiple databases
            search_term = params.get("search_term")
            databases = params.get("databases") or list(DATABASE_CONFIGS.keys())
            table_pattern = params.get("table_pattern", None)
            
            results = []
            for db in databases:
                try:
                    if table_pattern:
                        # Search in specific table pattern
                        search_query = f"""
                        SELECT table_name FROM information_schema.tables 
                        WHERE table_name LIKE '%{table_pattern}%' 
                        AND table_schema = 'public'
                        """
                        tables = run_query(search_query, db)
                        
                        db_results = []
                        for table_row in tables:
                            table_name = table_row['table_name']
                            try:
                                # Get text columns for this table
                                col_query = f"""
                                SELECT column_name FROM information_schema.columns 
                                WHERE table_name = '{table_name}' 
                                AND data_type IN ('text', 'varchar', 'character varying')
                                """
                                columns = run_query(col_query, db)
                                
                                if columns:
                                    # Search in text columns
                                    col_names = [col['column_name'] for col in columns]
                                    conditions = ' OR '.join([f"{col}::text ILIKE '%{search_term}%'" for col in col_names])
                                    search_query = f"SELECT * FROM {table_name} WHERE {conditions} LIMIT 10"
                                    table_results = run_query(search_query, db)
                                    
                                    if table_results:
                                        db_results.append({
                                            "table": table_name,
                                            "matches": table_results
                                        })
                            except:
                                continue
                        
                        results.append({
                            "database": db,
                            "results": db_results
                        })
                    else:
                        # General search across all text fields
                        general_search = f"""
                        SELECT 
                            t.table_name,
                            array_agg(c.column_name) as text_columns
                        FROM information_schema.tables t
                        JOIN information_schema.columns c ON t.table_name = c.table_name
                        WHERE t.table_schema = 'public' 
                        AND c.data_type IN ('text', 'varchar', 'character varying')
                        GROUP BY t.table_name
                        LIMIT 20
                        """
                        
                        tables_with_text = run_query(general_search, db)
                        db_results = []
                        
                        for table_info in tables_with_text:
                            table_name = table_info['table_name']
                            text_columns = table_info['text_columns']
                            
                            try:
                                conditions = ' OR '.join([f"{col}::text ILIKE '%{search_term}%'" for col in text_columns])
                                search_query = f"SELECT * FROM {table_name} WHERE {conditions} LIMIT 5"
                                table_results = run_query(search_query, db)
                                
                                if table_results:
                                    db_results.append({
                                        "table": table_name,
                                        "matches": table_results
                                    })
                            except:
                                continue
                        
                        results.append({
                            "database": db,
                            "results": db_results
                        })
                        
                except Exception as e:
                    results.append({
                        "database": db,
                        "error": str(e)
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

        elif method == "get_database_schema":
            database = params.get("database", "primary")
            table_name = params.get("table_name", None)
            
            if table_name:
                # Get columns for specific table
                schema_query = """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
                """
                rows = run_query_with_params(schema_query, [table_name], database)
            else:
                # Get all tables
                schema_query = """
                SELECT table_name, table_type 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
                """
                rows = run_query(schema_query, database)
            
            return {
                "jsonrpc": "2.0",
                "id": rpc.id,
                "result": {
                    "database": database,
                    "table_name": table_name,
                    "schema": rows
                }
            }

        elif method == "list_databases":
            return {
                "jsonrpc": "2.0",
                "id": rpc.id,
                "result": {
                    "databases": list(DATABASE_CONFIGS.keys()),
                    "count": len(DATABASE_CONFIGS)
                }
            }

        elif method == "cross_reference_search":
            # Search for related data across databases using common fields
            reference_value = params.get("reference_value")
            reference_fields = params.get("reference_fields", ["id", "user_id", "customer_id", "email"])
            databases = params.get("databases") or list(DATABASE_CONFIGS.keys())
            
            cross_results = {}
            
            for db in databases:
                try:
                    db_results = []
                    for field in reference_fields:
                        # Find tables with this field
                        field_query = f"""
                        SELECT DISTINCT table_name 
                        FROM information_schema.columns 
                        WHERE column_name = '{field}' AND table_schema = 'public'
                        """
                        tables = run_query(field_query, db)
                        
                        for table_row in tables:
                            table_name = table_row['table_name']
                            try:
                                # Search for the reference value
                                search_query = f"""
                                SELECT * FROM {table_name} 
                                WHERE {field}::text = '{reference_value}' 
                                LIMIT 5
                                """
                                matches = run_query(search_query, db)
                                
                                if matches:
                                    db_results.append({
                                        "table": table_name,
                                        "field": field,
                                        "matches": matches
                                    })
                            except:
                                continue
                    
                    cross_results[db] = db_results
                    
                except Exception as e:
                    cross_results[db] = {"error": str(e)}
            
            return {
                "jsonrpc": "2.0",
                "id": rpc.id,
                "result": {
                    "reference_value": reference_value,
                    "cross_reference_results": cross_results
                }
            }

        else:
            available_methods = [
                "fetch_data", "execute_query", "search_across_databases", 
                "get_database_schema", "list_databases", "cross_reference_search"
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


def get_database_url(database: str) -> str:
    """Get database URL by name"""
    if database not in DATABASE_CONFIGS:
        available = list(DATABASE_CONFIGS.keys())
        raise ValueError(f"Database '{database}' not found. Available databases: {available}")
    if not DATABASE_CONFIGS[database]:
        raise ValueError(f"Database '{database}' URL is not configured")
    return DATABASE_CONFIGS[database]


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
    
    for db_name, db_url in DATABASE_CONFIGS.items():
        try:
            with psycopg.connect(db_url, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    health_status[db_name] = "healthy"
        except Exception as e:
            health_status[db_name] = f"unhealthy: {str(e)}"
    
    return {
        "status": "ok", 
        "databases": health_status,
        "total_databases": len(DATABASE_CONFIGS)
    }


if __name__ == "__main__":
    import uvicorn
    print("Starting Multi-Database MCP Server for Deep Search...")
    print(f"Available databases: {list(DATABASE_CONFIGS.keys())}")
    uvicorn.run(app, host="0.0.0.0", port=8000)
