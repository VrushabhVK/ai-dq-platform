# AI Data Quality Platform (POC)

An end-to-end **AI-powered data quality tool** built with **Python + Streamlit**, designed to:
- Profile datasets
- Detect anomalies and duplicates
- Suggest data quality rules using an LLM
- Manage custom rule sets
- Generate downloadable **PDF validation reports**
- Ingest data from **Snowflake / PostgreSQL / MySQL** or file uploads

---

## Features

### 🔍 Data Profiling
- Nulls, distinct counts, and basic statistics per column
- Overall **Data Quality Score (0–100)**

### ⚠️ Anomaly & Duplicate Detection
- Isolation Forest–based **numeric anomaly detection**
- Fuzzy duplicate detection using `rapidfuzz`

### 🧠 AI Rule Suggestions
- LLM-based (or heuristic fallback) rule suggestions per column  
  e.g. regex for emails, uniqueness constraints, null rate thresholds.

### ⚙️ Rule Management UI
- View, add, and delete rules in the **Rules** tab
- Persist rules in a relational database (SQLite/Postgres/MySQL via SQLAlchemy)

### 📥 Data Ingestion
- Upload CSV / Excel
- Connect to:
  - **PostgreSQL**
  - **MySQL**
  - **Snowflake** (requires extra drivers)
- Load via table name or custom SQL query

### 📄 PDF Validation Reports
- One-click PDF generation from the **Reports** tab
- Includes:
  - Summary
  - Profiling snapshot
  - Active rules

---

## Project Structure

```text
ai-dq-platform/
├── app/
│   ├── streamlit_app.py       # Streamlit UI
│   └── run_scheduler.py       # (optional) example scheduler runner
├── dqtool/
│   ├── __init__.py
│   ├── profiling.py           # profiling logic
│   ├── anomalies.py           # anomaly detection
│   ├── duplicates.py          # duplicate detection
│   ├── imputation.py          # KNN imputation
│   ├── scoring.py             # DQ score calculation
│   ├── ingestion.py           # Snowflake/Postgres/MySQL loading
│   ├── reports.py             # PDF report generation
│   ├── db.py                  # DB persistence (scans, rules, reports)
│   └── scheduler.py           # (optional) APScheduler jobs
├── tests/
│   └── test_profiling.py
├── requirements.txt
├── Dockerfile
└── README.md
