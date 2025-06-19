from fastapi import FastAPI, Request, HTTPException, Header
from pydantic import BaseModel
import psycopg
import os
import json
from dotenv import load_dotenv

load_dotenv()  # Load DB creds from .env

app = FastAPI()
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
    if authorization != f"Bearer {AUTH_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")

    body = await req.json()
    try:
        rpc = JsonRpcRequest(**body)
        method = rpc.method
        params = rpc.params
        query = params.get("query")
    except Exception as e:
        return {"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid request"}, "id": None}

    try:
        if method == "fetch_data":
            rows = run_query(query)
            return {"jsonrpc": "2.0", "id": rpc.id, "result": {"rows": rows}}

        elif method == "execute_query":
            row_count = run_exec(query)
            return {"jsonrpc": "2.0", "id": rpc.id, "result": {"row_count": row_count}}

        else:
            return {"jsonrpc": "2.0", "error": {"code": -32601, "message": "Method not found"}, "id": rpc.id}

    except Exception as e:
        return {"jsonrpc": "2.0", "error": {"code": -32000, "message": str(e)}, "id": rpc.id}


def run_query(query: str):
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            colnames = [desc[0] for desc in cur.description]
            return [dict(zip(colnames, row)) for row in cur.fetchall()]

def run_exec(query: str):
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.rowcount
