import psycopg2
from config import DB_URL

def get_db_connection():
    return psycopg2.connect(DB_URL)

def close_connection(conn, cur):
    cur.close()
    conn.close()