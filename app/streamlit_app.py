# app/streamlit_app.py
import sys
import os

# Ensure project root is on sys.path so "dqtool" can be imported
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd

from dqtool.profiling import profile_dataframe
from dqtool.anomalies import detect_anomalies_numeric
from dqtool.duplicates import detect_duplicates_simple
from dqtool.imputation import impute_missing_knn
from dqtool.scoring import compute_dq_score
from dqtool.llm import suggest_rules
from dqtool.ingestion import load_from_source
from dqtool.reports import generate_pdf_report_bytes
from dqtool.db import init_db, save_scan, save_rules, load_rules, save_report_meta

# Initialize DB (creates tables if not exist)
init_db()

st.set_page_config(page_title="AI DQ Platform", layout="wide")
st.title("AI Data Quality Platform — POC")

st.markdown(
    """
This app lets you:

- Upload data **or** connect to a DB (Snowflake / Postgres / MySQL)
- Run **data profiling, anomaly detection, duplicates, and imputation**
- Get **LLM-suggested data quality rules**
- Manage your **own rule set**
- Generate **PDF validation reports**
"""
)

# ----------------- Data source selection -----------------
source_tab, dq_tab, rules_tab, reports_tab = st.tabs(
    ["📥 Data Source", "📊 Data Quality", "⚙️ Rules", "📄 Reports"]
)

# We keep df + profile in session_state so all tabs can use it
if "df" not in st.session_state:
    st.session_state.df = None
if "profile_df" not in st.session_state:
    st.session_state.profile_df = None
if "dq_score" not in st.session_state:
    st.session_state.dq_score = None
if "rules" not in st.session_state:
    # load from DB on first run
    st.session_state.rules = load_rules() or []

# -------------- TAB 1: Data Source --------------
with source_tab:
    st.subheader("Upload file or connect to a database")

    source_mode = st.radio(
        "Select source type",
        ["Upload CSV/Excel", "Database (Snowflake / Postgres / MySQL)"],
        horizontal=True,
    )

    df = None

    if source_mode == "Upload CSV/Excel":
        uploaded = st.file_uploader("Upload CSV or Excel", type=["csv", "xlsx", "xls"])
        if uploaded is not None:
            try:
                df = pd.read_csv(uploaded)
            except Exception:
                uploaded.seek(0)
                df = pd.read_excel(uploaded)

            st.write("**Preview (first 100 rows)**")
            st.dataframe(df.head(100))
    else:
        st.markdown("### Database connection")
        db_type = st.selectbox(
            "Database type", ["PostgreSQL", "MySQL", "Snowflake"]
        )

        col1, col2 = st.columns(2)
        with col1:
            host = st.text_input("Host / Account (for Snowflake: account)", "")
            user = st.text_input("User")
            database = st.text_input("Database / Schema")
        with col2:
            port = st.text_input("Port (ignored for Snowflake)", "5432")
            password = st.text_input("Password", type="password")
            table_or_query = st.text_area(
                "Table name OR SQL query",
                "SELECT * FROM your_table LIMIT 1000",
            )

        if st.button("Load from database"):
            with st.spinner("Connecting and loading data..."):
                try:
                    df = load_from_source(
                        db_type=db_type,
                        host=host,
                        port=port,
                        user=user,
                        password=password,
                        database=database,
                        table_or_query=table_or_query,
                    )
                    st.success(f"Loaded {len(df)} rows.")
                    st.dataframe(df.head(100))
                except Exception as e:
                    st.error(f"Failed to load data: {e}")

    # Save df to session_state
    if df is not None:
        st.session_state.df = df
        st.info("Data loaded. Now open the **Data Quality** tab.")

# -------------- TAB 2: Data Quality --------------
with dq_tab:
    st.subheader("Data Quality Analysis")

    df = st.session_state.df
    if df is None:
        st.warning("No data loaded yet. Go to **Data Source** tab first.")
    else:
        # Profiling
        profile_df = profile_dataframe(df)
        st.session_state.profile_df = profile_df

        st.markdown("#### Profiling summary")
        st.dataframe(profile_df)

        dq_score = compute_dq_score(profile_df)
        st.session_state.dq_score = dq_score
        st.metric("Data Quality Score", f"{dq_score} / 100")

        # Anomalies
        st.markdown("#### Anomaly Detection")
        contamination = st.slider(
            "Expected anomaly rate", min_value=0.0, max_value=0.2, value=0.01, step=0.005
        )
        anom_series = detect_anomalies_numeric(df, contamination=contamination)
        # anom_series is a Series[bool]; ensure scalar for condition
        anomalous_rows = df[anom_series] if anom_series.any() else df.iloc[0:0]
        st.write(f"Detected anomalies: **{len(anomalous_rows)}**")
        if len(anomalous_rows) > 0:
            st.dataframe(anomalous_rows.head(100))

        # Duplicates
        st.markdown("#### Duplicate Detection")
        dup_series = detect_duplicates_simple(df)
        duplicate_rows = df[dup_series] if dup_series.any() else df.iloc[0:0]
        st.write(f"Potential duplicates: **{len(duplicate_rows)}**")
        if len(duplicate_rows) > 0:
            st.dataframe(duplicate_rows.head(100))

        # Imputation
        st.markdown("#### Missing Value Imputation (numeric KNN)")
        if st.button("Run KNN Imputation (preview)"):
            imputed = impute_missing_knn(df)
            st.dataframe(imputed.head(20))

        # Save scan metadata
        if st.button("Save this scan to DB"):
            try:
                scan_report = {
                    "profile": profile_df.to_dict(orient="records"),
                    "anomalies_count": int(len(anomalous_rows)),
                    "duplicates_count": int(len(duplicate_rows)),
                }
                save_scan(
                    job_name="manual_streamlit_scan",
                    row_count=len(df),
                    dq_score=float(dq_score),
                    report=scan_report,
                )
                st.success("Scan saved to database.")
            except Exception as e:
                st.error(f"Failed to save scan: {e}")

# -------------- TAB 3: Rules --------------
with rules_tab:
    st.subheader("Rule Management")

    profile_df = st.session_state.profile_df
    if profile_df is None:
        st.info("Run data quality profiling first (Data Quality tab).")
    else:
        col_suggest, col_current = st.columns(2)

        with col_suggest:
            st.markdown("#### LLM-Suggested Rules")
            if st.button("Generate suggestions"):
                with st.spinner("Calling LLM / heuristic rule generator..."):
                    try:
                        llm_rules = suggest_rules(profile_df)
                        st.session_state.rules.extend(llm_rules)
                        # remove duplicates (by column + rule text)
                        unique = {
                            (r.get("column"), r.get("rule")): r
                            for r in st.session_state.rules
                        }
                        st.session_state.rules = list(unique.values())
                        save_rules(st.session_state.rules)
                        st.success(f"Added {len(llm_rules)} suggested rules.")
                    except Exception as e:
                        st.error(f"Failed to generate rules: {e}")

        with col_current:
            st.markdown("#### Current Rules")
            rules = st.session_state.rules or []
            if not rules:
                st.info("No rules saved yet.")
            else:
                for idx, r in enumerate(rules):
                    with st.expander(f"Rule #{idx+1} — {r.get('column', 'N/A')}"):
                        st.write(f"**Column:** {r.get('column')}")
                        st.write(f"**Rule:** `{r.get('rule')}`")
                        st.write(f"**Confidence:** {r.get('confidence', 'N/A')}")
                        if st.button(f"Delete rule #{idx+1}", key=f"del_rule_{idx}"):
                            rules.pop(idx)
                            st.session_state.rules = rules
                            save_rules(rules)
                            st.experimental_rerun()

        st.markdown("#### Add custom rule")
        with st.form("add_rule_form"):
            col_name = st.text_input("Column name")
            rule_text = st.text_area("Rule description / expression")
            conf = st.slider("Confidence", 0.0, 1.0, 0.8)
            submitted = st.form_submit_button("Add rule")
            if submitted:
                if not col_name or not rule_text:
                    st.error("Column and rule text are required.")
                else:
                    new_rule = {
                        "column": col_name,
                        "rule": rule_text,
                        "confidence": float(conf),
                    }
                    st.session_state.rules.append(new_rule)
                    save_rules(st.session_state.rules)
                    st.success("Rule added.")

# -------------- TAB 4: Reports --------------
with reports_tab:
    st.subheader("Automated Validation Reports (PDF)")

    df = st.session_state.df
    profile_df = st.session_state.profile_df
    dq_score = st.session_state.dq_score
    rules = st.session_state.rules or []

    if df is None or profile_df is None or dq_score is None:
        st.info("Run a data quality analysis first.")
    else:
        if st.button("Generate PDF report"):
            with st.spinner("Generating PDF report..."):
                try:
                    pdf_bytes = generate_pdf_report_bytes(
                        df=df,
                        profile_df=profile_df,
                        dq_score=dq_score,
                        rules=rules,
                    )
                    # Save report metadata
                    report_id = save_report_meta(
                        title="DQ Report (Streamlit)",
                        dq_score=float(dq_score),
                        row_count=len(df),
                    )
                    st.success(f"Report generated and saved (ID: {report_id}).")
                    st.download_button(
                        label="Download PDF report",
                        data=pdf_bytes,
                        file_name="dq_report.pdf",
                        mime="application/pdf",
                    )
                except Exception as e:
                    st.error(f"Failed to generate report: {e}")
