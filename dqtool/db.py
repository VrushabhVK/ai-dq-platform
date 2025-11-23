# dqtool/db.py
import os
import json
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, JSON, DateTime
from sqlalchemy.sql import func
from sqlalchemy.exc import OperationalError, SQLAlchemyError

# Optional DB: use SQLite by default, but switch to in-memory fallback if DB fails
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///dq_meta.db")

DB_AVAILABLE = True
engine = None
metadata = MetaData()

try:
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
except Exception:
    DB_AVAILABLE = False


# ---------------------------
# Define tables
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
# Init DB safely
# ---------------------------

def init_db():
    global DB_AVAILABLE
    if not engine:
        DB_AVAILABLE = False
        return

    try:
        metadata.create_all(engine)
    except Exception:
        DB_AVAILABLE = False


# ---------------------------
# Scans
# ---------------------------

def save_scan(job_name, row_count, dq_score, report):
    if not DB_AVAILABLE:
        return  # fail silently on Streamlit Cloud

    try:
        with engine.connect() as conn:
            conn.execute(
                scans.insert().values(
                    job_name=job_name,
                    row_count=row_count,
                    dq_score=dq_score,
                    report=report,
                )
            )
    except Exception:
        pass  # fallback silently


# ---------------------------
# Rules
# ---------------------------

def save_rules(rules):
    if not DB_AVAILABLE:
        return

    try:
        with engine.connect() as conn:
            conn.execute(
                rules_table.insert().values(rules_json=rules)
            )
    except Exception:
        pass


def load_rules():
    if not DB_AVAILABLE:
        return []  # fallback

    try:
        with engine.connect() as conn:
            row = conn.execute(
                rules_table.select().order_by(rules_table.c.id.desc())
            ).fetchone()
            return row["rules_json"] if row else []
    except Exception:
        return []


# ---------------------------
# Reports
# ---------------------------

def save_report_meta(title, dq_score, row_count):
    if not DB_AVAILABLE:
        return 0  # fake ID

    try:
        with engine.connect() as conn:
            res = conn.execute(
                reports_table.insert().values(
                    title=title,
                    dq_score=dq_score,
                    row_count=row_count,
                )
            )
            return res.inserted_primary_key[0]
    except Exception:
        return 0
