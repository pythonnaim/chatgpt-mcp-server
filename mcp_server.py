from flask import Flask, request, jsonify
import psycopg2
import os

app = Flask(__name__)

# Load DB credentials from environment variables
DB_HOST = os.getenv('PGHOST')
DB_PORT = os.getenv('PGPORT', 25060)
DB_NAME = os.getenv('PGDATABASE')
DB_USER = os.getenv('PGUSER')
DB_PASS = os.getenv('PGPASSWORD')
BEARER_TOKEN = os.getenv('MCP_TOKEN')

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        sslmode='require'
    )

def validate_auth(req):
    auth_header = req.headers.get("Authorization", "")
    return auth_header == f"Bearer {BEARER_TOKEN}"

@app.route("/execute", methods=["POST"])
def execute():
    if not validate_auth(request):
        return jsonify({"error": "Unauthorized"}), 401

    payload = request.get_json()
    query = payload.get("query")

    if not query:
        return jsonify({"error": "Missing SQL query"}), 400

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        data = [dict(zip(columns, row)) for row in rows]
        cur.close()
        conn.close()
        return jsonify({"data": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({"status": "ok"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
