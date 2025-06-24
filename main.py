from fastapi import FastAPI, Request, HTTPException, HeaderMore actions
from pydantic import BaseModel
import psycopg
import os
import json
from dotenv import load_dotenv

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
DB_URL = os.getenv("POSTGRES_URL")  # e.g. "postgresql://user:pass@host:25060/dbname"

# --- JSON-RPC Request format ---
class JsonRpcRequest(BaseModel):
    jsonrpc: str
    method: str
    id: int
    params: dict

@app.post("/mcp")
async def mcp_handler(req: Request, authorization: str = Header(None)):
    # Uncomment this in production to enforce authorization
    # if authorization != f"Bearer {AUTH_TOKEN}":
    #     raise HTTPException(status_code=401, detail="Unauthorized")

    body = await req.json()
    try:
        body = await req.json()
        rpc = JsonRpcRequest(**body)
        method = rpc.method
        params = rpc.params
        query = params.get("query")
        db_name = params.get("db_name")
        
        if not db_name:
            raise HTTPException(status_code=400, detail="Missing 'db_name' in params")

        # Construct full DB URL by replacing database name
        base_url = DB_URL.rsplit("/", 1)[0]
        full_db_url = f"{base_url}/{db_name}"

    except Exception as e:
        return {
            "jsonrpc": "2.0",
            "error": {"code": -32600, "message": "Invalid request"},
            "id": None
        }
        return {"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid request"}, "id": None}

    try:
        if method == "fetch_data":
            rows = run_query(query, full_db_url)
            rows = run_query(query)
            return {"jsonrpc": "2.0", "id": rpc.id, "result": {"rows": rows}}

        elif method == "execute_query":
            row_count = run_exec(query, full_db_url)
            row_count = run_exec(query)
            return {"jsonrpc": "2.0", "id": rpc.id, "result": {"row_count": row_count}}

        else:
            return {
                "jsonrpc": "2.0",
                "error": {"code": -32601, "message": "Method not found"},
                "id": rpc.id
            }
            return {"jsonrpc": "2.0", "error": {"code": -32601, "message": "Method not found"}, "id": rpc.id}

    except Exception as e:
        return {
            "jsonrpc": "2.0",
            "error": {"code": -32000, "message": str(e)},
            "id": rpc.id
        }
        return {"jsonrpc": "2.0", "error": {"code": -32000, "message": str(e)}, "id": rpc.id}


def run_query(query: str, db_url: str):
    with psycopg.connect(db_url, autocommit=True) as conn:
def run_query(query: str):
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            colnames = [desc[0] for desc in cur.description]
            return [dict(zip(colnames, row)) for row in cur.fetchall()]


def run_exec(query: str, db_url: str):
    with psycopg.connect(db_url, autocommit=True) as conn:
def run_exec(query: str):
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.rowcount
