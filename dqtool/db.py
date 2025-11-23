# dqtool/db.py
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    JSON,
    DateTime,
)
from sqlalchemy.sql import func, select
import os
import json

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

rules_table = Table(
    "rules",
    meta,
    Column("id", Integer, primary_key=True),
    Column("created_at", DateTime, server_default=func.now()),
    Column("rule_json", JSON),
)

reports_table = Table(
    "reports",
    meta,
    Column("id", Integer, primary_key=True),
    Column("created_at", DateTime, server_default=func.now()),
    Column("title", String),
    Column("dq_score", Integer),
    Column("row_count", Integer),
)


def init_db():
    meta.create_all(engine)


def save_scan(job_name: str, row_count: int, dq_score: float, report: dict):
    with engine.begin() as conn:
        conn.execute(
            scans.insert().values(
                job_name=job_name,
                row_count=row_count,
                dq_score=int(dq_score),
                report=report,
            )
        )


def save_rules(rules: list[dict]):
    """Overwrite all rules."""
    with engine.begin() as conn:
        conn.execute(rules_table.delete())
        for r in rules:
            conn.execute(rules_table.insert().values(rule_json=r))


def load_rules() -> list[dict] | None:
    with engine.begin() as conn:
        result = conn.execute(select(rules_table.c.rule_json))
        rows = result.fetchall()
        if not rows:
            return []
        return [json.loads(json.dumps(r[0])) for r in rows]


def save_report_meta(title: str, dq_score: float, row_count: int) -> int:
    with engine.begin() as conn:
        res = conn.execute(
            reports_table.insert().values(
                title=title,
                dq_score=int(dq_score),
                row_count=int(row_count),
            )
        )
        return res.inserted_primary_key[0]
