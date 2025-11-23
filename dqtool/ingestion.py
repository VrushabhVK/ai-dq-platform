# dqtool/ingestion.py
"""
Data ingestion helpers for Snowflake / PostgreSQL / MySQL.

All functions return a pandas DataFrame.
"""

import pandas as pd
import sqlalchemy as sa


def _make_sqlalchemy_engine(db_type: str, host: str, port: str, user: str, password: str, database: str):
    db_type = db_type.lower()
    if db_type == "postgresql":
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    elif db_type == "mysql":
        url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    elif db_type == "snowflake":
        # host is actually "account" for Snowflake
        # requires: pip install snowflake-sqlalchemy snowflake-connector-python
        url = f"snowflake://{user}:{password}@{host}/{database}"
    else:
        raise ValueError(f"Unsupported db_type: {db_type}")
    return sa.create_engine(url)


def load_from_source(
    db_type: str,
    host: str,
    port: str,
    user: str,
    password: str,
    database: str,
    table_or_query: str,
) -> pd.DataFrame:
    """
    db_type: 'PostgreSQL' | 'MySQL' | 'Snowflake'
    table_or_query: either a plain table name or a full SQL query
    """
    engine = _make_sqlalchemy_engine(db_type, host, port, user, password, database)

    with engine.connect() as conn:
        text = table_or_query.strip()
        if " " in text.lower():
            # treat as SQL
            df = pd.read_sql(text, conn)
        else:
            # treat as table name
            df = pd.read_sql(f"SELECT * FROM {text}", conn)

    return df
