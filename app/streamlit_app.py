# app/streamlit_app.py
import sys
import os

# Ensure project root is on sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd

from dqtool.profiling import profile_dataframe
from dqtool.anomalies import detect_anomalies_numeric
from dqtool.duplicates import find_duplicates_advanced
from dqtool.imputation import impute_missing_knn
from dqtool.scoring import compute_dq_score
from dqtool.llm import suggest_rules
from dqtool.db import init_db, save_scan, save_rules, load_rules

# 🔥 TEMP placeholders for modules not created yet
def load_from_source(*args, **kwargs):
    st.error("Database ingestion module not implemented yet.")
    return pd.DataFrame()

def generate_pdf_report_bytes(*args, **kwargs):
    st.error("PDF report module not implemented yet.")
    return b"%PDF-1.4\n% Fake PDF Placeholder\n"

def save_report_meta(*args, **kwargs):
    return 1  # dummy ID


# Initialize DB
init_db()

st.set_page_config(page_title="AI DQ Platform", layout="wide")
st.title("AI Data Quality Platform — POC")


# Tabs
source_tab, dq_tab, rules_tab, reports_tab = st.tabs(
    ["📥 Data Source", "📊 Data Quality", "⚙️ Rules", "📄 Reports"]
)

# Session state
if "df" not in st.session_state:
    st.session_state.df = None
if "rules" not in st.session_state:
    st.session_state.rules = load_rules() or []

# ---------------- TAB 1: Data Source ----------------
with source_tab:
    st.subheader("Upload CSV/Excel")
    uploaded = st.file_uploader("Upload file", type=["csv", "xlsx", "xls"])

    if uploaded:
        try:
            df = pd.read_csv(uploaded)
        except Exception:
            uploaded.seek(0)
            df = pd.read_excel(uploaded)

        st.session_state.df = df
        st.success("File loaded successfully.")
        st.dataframe(df.head(100))

# ---------------- TAB 2: Data Quality ----------------
with dq_tab:
    df = st.session_state.df
    if df is None:
        st.warning("Upload data first in the Data Source tab.")
    else:
        st.subheader("Profiling")
        profile_df = profile_dataframe(df)
        st.dataframe(profile_df)

        dq_score = compute_dq_score(profile_df)
        st.metric("DQ Score", f"{dq_score} / 100")

        # Anomalies
        st.subheader("Anomaly Detection")
        contamination = st.slider("Expected anomaly rate", 0.0, 0.2, 0.01, 0.005)
        anom_series = detect_anomalies_numeric(df, contamination)
        anomaly_rows = df[anom_series] if anom_series.any() else df.iloc[0:0]
        st.write(f"Anomalies found: {len(anomaly_rows)}")
        if len(anomaly_rows) > 0:
            st.dataframe(anomaly_rows)

        # Duplicates
        st.subheader("Enterprise Duplicate Detection")
        subset_cols = st.multiselect(
            "Columns to use for duplicate matching",
            df.columns.tolist(),
            default=[c for c in df.columns if df[c].dtype == "object"][:3]
        )
        threshold = st.slider("Similarity threshold", 70, 100, 90)

        dup_mask, pairs_df = find_duplicates_advanced(df, subset_cols, threshold)
        duplicate_rows = df[dup_mask]

        st.write(f"Duplicate rows found: {dup_mask.sum()}")
        if not pairs_df.empty:
            st.write("Duplicate groups:")
            st.dataframe(pairs_df)
            st.write("Full duplicate rows:")
            st.dataframe(duplicate_rows)
        else:
            st.info("No duplicates detected.")

        # Imputation
        st.subheader("Missing Value Imputation")
        if st.button("Run KNN Imputation (preview)"):
            imputed = impute_missing_knn(df)
            st.dataframe(imputed.head(20))

        # Save Scan
        if st.button("Save Scan to DB"):
            try:
                report_data = {
                    "profile": profile_df.to_dict(orient="records"),
                    "anomalies_count": len(anomaly_rows),
                    "duplicates_count": len(duplicate_rows),
                }
                save_scan(
                    job_name="manual_streamlit_scan",
                    row_count=len(df),
                    dq_score=float(dq_score),
                    report=report_data,
                )
                st.success("Scan saved.")
            except Exception as e:
                st.error(f"Error saving scan: {e}")

# ---------------- TAB 3: Rules ----------------
with rules_tab:
    st.subheader("Rule Management")

    # Generate suggestions
    if st.button("Generate LLM Rules"):
        llm_rules = suggest_rules(profile_dataframe(st.session_state.df))
        st.session_state.rules.extend(llm_rules)
        save_rules(st.session_state.rules)
        st.success(f"Added {len(llm_rules)} rules.")

    # Display rules
    rules = st.session_state.rules
    if not rules:
        st.info("No rules yet.")
    else:
        for i, r in enumerate(rules):
            with st.expander(f"Rule #{i+1} – {r['column']}"):
                st.json(r)
                if st.button(f"Delete Rule #{i+1}", key=f"del_{i}"):
                    rules.pop(i)
                    save_rules(rules)
                    st.experimental_rerun()

# ---------------- TAB 4: Reports ----------------
with reports_tab:
    st.subheader("PDF Validation Report")
    if st.button("Generate Report"):
        pdf_bytes = generate_pdf_report_bytes()
        st.download_button(
            "Download PDF",
            pdf_bytes,
            file_name="dq_report.pdf",
            mime="application/pdf"
        )
