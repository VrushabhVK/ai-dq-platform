# dqtool/ingestion.py
import pandas as pd
import sqlalchemy
import snowflake.connector
import pymysql


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
    Load data from Snowflake, PostgreSQL, or MySQL.
    """

    db_type = db_type.lower()

    # -----------------------------
    # PostgreSQL
    # -----------------------------
    if db_type == "postgresql":
        engine = sqlalchemy.create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{database}"
        )
        return pd.read_sql(table_or_query, engine)

    # -----------------------------
    # MySQL
    # -----------------------------
    if db_type == "mysql":
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        )
        return pd.read_sql(table_or_query, engine)

    # -----------------------------
    # Snowflake
    # -----------------------------
    if db_type == "snowflake":
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=host,
            warehouse="COMPUTE_WH",
            database=database,
            schema="PUBLIC",
        )

        df = pd.read_sql(table_or_query, conn)
        conn.close()
        return df

    raise ValueError("Unsupported database type. Use PostgreSQL, MySQL, or Snowflake.")
