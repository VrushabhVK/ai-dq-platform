# dqtool/db.py
import os

from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime,
)
from sqlalchemy.sql import func
from sqlalchemy.types import JSON

# Default: local sqlite file (works even without Postgres)
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///dq_meta.db")

engine = create_engine(DATABASE_URL)
meta = MetaData()

scans = Table(
    "scans",
    meta,
    Column("id", Integer, primary_key=True),
    Column("job_name", String, nullable=False),
    Column("run_at", DateTime, server_default=func.now()),
    Column("row_count", Integer),
    Column("dq_score", Integer),
    Column("report", JSON),
)


def init_db() -> None:
    """
    Create tables if they don't exist.
    """
    meta.create_all(engine)


def save_scan(job_name: str, row_count: int, dq_score: float, report: dict) -> None:
    """
    Persist a scan result to DB.
    """
    with engine.begin() as conn:
        conn.execute(
            scans.insert().values(
                job_name=job_name,
                row_count=row_count,
                dq_score=int(dq_score),
                report=report,
            )
        )
