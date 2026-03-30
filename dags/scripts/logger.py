from sqlalchemy import text
from scripts.sql_queries import SQL_LOG_START, SQL_LOG_SUCCESS, SQL_LOG_FAIL

def log_start(conn, pipeline_name): 
    return conn.execute(text(SQL_LOG_START), {"pipeline_name": pipeline_name}).scalar()

def log_success(conn, log_id, records): 
    conn.execute(text(SQL_LOG_SUCCESS), {"records": records, "log_id": log_id})

def log_fail(conn, log_id, err_msg): 
    conn.execute(text(SQL_LOG_FAIL), {"err_msg": err_msg[:2000], "log_id": log_id})