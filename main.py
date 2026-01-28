from dotenv import load_dotenv
load_dotenv()

# 1. Standard library imports (Python built-ins)
from datetime import datetime as dt, timezone
import logging
from multiprocessing import Pool
import os
from sys import platform
import uuid

# 2. Third-party imports
from flask import Flask, request, Request, jsonify
from sqlalchemy import text
import google.cloud.logging

# 3. Local application/library specific imports
from processes.create_table import connection_postgresql
from processes.create_table import create_table


PHASE = "create_tables"
LEVELS_MAP = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}
LOG_LEVEL = LEVELS_MAP.get(os.environ.get("LOG_LEVEL","debug").lower())
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main(request: Request):
    try:
        ingestion_timestamp = dt.now(tz=timezone.utc)
        execution_id = str(uuid.uuid4())
        request_data = request.get_json()
        system = request_data["system"].upper()
        batch_id = f"{system}_{ingestion_timestamp.strftime('%Y%m%d%H%M%S')}"
        
    
        if platform not in ["win32", "win64"]:
            client = google.cloud.logging.Client()
            client.setup_logging(log_level=LOG_LEVEL, labels={"execution_id":execution_id, "phase":PHASE, "system":system, "batch_id":batch_id})
        
        logging.info("Starting phase %s", PHASE)
        
        session = connection_postgresql()
        
        # Searching in control table all the tables that need to be created
        tables_to_be_created = session.execute(text("SELECT codigo, tabela_origem, sistema_origem, schema_origem FROM controle WHERE status = 'ativo' and tipo='database' and tipo_banco_origem='mysql' and bigquery_create_table = 'S' "))
        rows_tables_to_be_created = tables_to_be_created.fetchall()
        
        session.close()
        
        if not rows_tables_to_be_created:
            logging.info("No tables found to be created.")
            return jsonify({"message": "No tables to process"}), 200
        
        tasks = [
            (row[0], row[1], row[2], row[3]) 
            for row in rows_tables_to_be_created
        ]
        
        # Parallel table creation
        with Pool(processes=min(int(os.environ.get("NUM_POOLS", 2)), len(tasks))) as pool:
            # tasks = [(system, table, schemas[table], sys_cols, ingestion_timestamp, batch_id, secrets) for table in request_data["tables"]]
            results = pool.starmap(create_table, tasks)
        
        compiled_results = {tasks[i][1]: results[i] for i in range(len(tasks))}
        if None in compiled_results.values():
            raise Exception("Error creating tables")
        
        logging.info("Finished phase %s", PHASE)
        status_code = 200
        result = {"execution_id":execution_id, "phase":PHASE, "system":system, "status":"success", "status_code":status_code, "batch_id":batch_id, "rows":compiled_results}
        
        
        
        
    except Exception as erro:
        logging.exception(f"Error: {erro}")
        status_code = 500
        result = {"execution_id":execution_id, "phase":PHASE, "system":system, "status":"error", "status_code":status_code, "error":str(erro), "batch_id":batch_id, "rows":compiled_results}
    
    return jsonify(result), status_code
    
if __name__ == "__main__":
    if platform in ["win32", "win64"]:
        
        app = Flask(__name__)
        @app.route('/',methods=['POST'])
        def local_tester():
            return main(request)
        
        app.run(host='0.0.0.0', port=8080, debug=True, load_dotenv=True, use_reloader=False)