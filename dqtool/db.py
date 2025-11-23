# dqtool/db.py
import os
import json
from sqlalchemy import (
    create_engine, MetaData, Table,
    Column, Integer, String, JSON, DateTime
)
from sqlalchemy.sql import func

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///dq_meta.db")

engine = create_engine(DATABASE_URL)
metadata = MetaData()

# ---------------------------
# Tables
# ---------------------------

scans = Table(
    "scans", metadata,
    Column("id", Integer, primary_key=True),
    Column("job_name", String),
    Column("row_count", Integer),
    Column("dq_score", Integer),
    Column("report", JSON),
    Column("created_at", DateTime, server_default=func.now()),
)

rules_table = Table(
    "rules", metadata,
    Column("id", Integer, primary_key=True),
    Column("rules_json", JSON),
    Column("created_at", DateTime, server_default=func.now()),
)

reports_table = Table(
    "reports", metadata,
    Column("id", Integer, primary_key=True),
    Column("title", String),
    Column("dq_score", Integer),
    Column("row_count", Integer),
    Column("created_at", DateTime, server_default=func.now()),
)


# ---------------------------
# Init DB
# ---------------------------

def init_db():
    metadata.create_all(engine)


# ---------------------------
# Scans
# ---------------------------

def save_scan(job_name, row_count, dq_score, report):
    with engine.connect() as conn:
        conn.execute(
            scans.insert().values(
                job_name=job_name,
                row_count=row_count,
                dq_score=dq_score,
                report=report,
            )
        )


# ---------------------------
# Rules
# ---------------------------

def save_rules(rules):
    with engine.connect() as conn:
        conn.execute(
            rules_table.insert().values(
                rules_json=rules
            )
        )


def load_rules():
    with engine.connect() as conn:
        result = conn.execute(rules_table.select().order_by(rules_table.c.id.desc()))
        row = result.fetchone()
        return row["rules_json"] if row else []


# ---------------------------
# Reports
# ---------------------------

def save_report_meta(title, dq_score, row_count):
    with engine.connect() as conn:
        res = conn.execute(
            reports_table.insert().values(
                title=title,
                dq_score=dq_score,
                row_count=row_count,
            )
        )
        return res.inserted_primary_key[0]
