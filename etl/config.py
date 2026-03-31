
# Central configuration — connection string + paths.
# In production (on-premise) these come from environment variables or a vault.


import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", "5433")),
    "dbname":   os.getenv("DB_NAME", "sales_analytics"),
    "user":     os.getenv("DB_USER", "dataeng"),
    "password": os.getenv("DB_PASSWORD", "dataeng123"),
}

DATA_DIR = BASE_DIR / "data" / "raw"

# DSN for psycopg2
def get_dsn() -> str:
    c = DB_CONFIG
    return (
        f"host={c['host']} port={c['port']} dbname={c['dbname']} "
        f"user={c['user']} password={c['password']}"
    )
