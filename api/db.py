import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    user = os.getenv("POSTGRES_USER", "carms")
    password = os.getenv("POSTGRES_PASSWORD", "carms")
    db = os.getenv("POSTGRES_DB", "carms")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    host = os.getenv("POSTGRES_HOST", "localhost")  # later in docker => "postgres"
    return psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
