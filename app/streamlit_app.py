# app/streamlit_app.py
import sys
import os

# Ensure project root is on sys.path so "dqtool" can be imported
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd

from dqtool.profiling import profile_dataframe
from dqtool.anomalies import detect_anomalies_numeric
from dqtool.duplicates import find_duplicates_advanced
from dqtool.imputation import impute_missing_knn
from dqtool.scoring import compute_dq_score
from dqtool.llm import suggest_rules
from dqtool.ingestion import load_from_source
from dqtool.reports import generate_pdf_report_bytes
from dqtool.db import (
    init_db,
    save_scan,
    save_rules,
    load_rules,
    save_report_meta,
    DB_AVAILABLE,
)

# Init DB (safe)
try:
    init_db()
except:
    pass

st.set_page_config(page_title="AI DQ Platform", layout="wide")
st.title("AI Data Quality Platform — POC")

# ---------------- Session state ----------------
if "df" not in st.session_state:
    st.session_state.df = None
if "profile" not in st.session_state:
    st.session_state.profile = None
if "dq_score" not in st.session_state:
    st.session_state.dq_score = None
if "rules" not in st.session_state:
    try:
        st.session_state.rules = load_rules() or []
    except:
        st.session_state.rules = []


# ---------------- Tabs ----------------
source_tab, dq_tab, rules_tab, reports_tab = st.tabs(
    ["📥 Data Source", "📊 Data Quality", "⚙️ Rules", "📄 Reports"]
)

# ---------------- TAB 1: Data Source ----------------
with source_tab:
    st.subheader("Upload CSV/Excel")

    uploaded = st.file_uploader("Upload file", type=["csv", "xlsx", "xls"])

    if uploaded:
        try:
            df = pd.read_csv(uploaded)
        except:
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
        st.subheader("Profiling Summary")
        profile_df = profile_dataframe(df)
        st.session_state.profile = profile_df
        st.dataframe(profile_df)

        # Score
        dq_score = compute_dq_score(profile_df)
        st.session_state.dq_score = dq_score
        st.metric("DQ Score", f"{dq_score} / 100")

        # Anomalies
        st.subheader("Anomaly Detection")
        contamination = st.slider("Contamination rate", 0.0, 0.2, 0.01, 0.005)
        anom_series = detect_anomalies_numeric(df, contamination)
        anom_rows = df[anom_series] if anom_series.any() else df.iloc[0:0]

        st.write(f"Anomalies detected: **{len(anom_rows)}**")
        if len(anom_rows) > 0:
            st.dataframe(anom_rows)

        # Duplicates
        st.subheader("Enterprise Duplicate Matching")

        subset_cols = st.multiselect(
            "Select columns for matching",
            df.columns.tolist(),
            default=[c for c in df.columns if df[c].dtype == "object"][:3]
        )

        threshold = st.slider("Similarity threshold", 70, 100, 90)

        dup_mask, pairs_df = find_duplicates_advanced(
            df,
            subset_cols=subset_cols or None,
            threshold=threshold,
        )

        duplicate_rows = df[dup_mask]
        st.write(f"Duplicate rows found: **{len(duplicate_rows)}**")

        if not pairs_df.empty:
            st.write("Duplicate Groups:")
            st.dataframe(pairs_df)
            st.write("Duplicate Rows:")
            st.dataframe(duplicate_rows)
        else:
            st.info("No duplicates found.")

        # Imputation preview
        st.subheader("Missing Value Imputation (KNN)")
        if st.button("Run KNN Imputation"):
            imputed = impute_missing_knn(df)
            st.dataframe(imputed.head(20))

        # Save scan to DB (safe)
        if st.button("Save Scan to DB"):
            try:
                scan_report = {
                    "profile": profile_df.to_dict(orient="records"),
                    "anomalies_count": len(anom_rows),
                    "duplicates_count": len(duplicate_rows),
                }
                save_scan(
                    job_name="manual_streamlit_scan",
                    row_count=len(df),
                    dq_score=float(dq_score),
                    report=scan_report,
                )
                st.success("Scan saved.")
            except:
                st.info("Database unavailable — scan saved in memory only.")


# ---------------- TAB 3: Rule Management ----------------
with rules_tab:
    st.subheader("Rule Management")

    # Generate rules
    if st.button("Generate LLM Rules"):
        df = st.session_state.df
        if df is None:
            st.warning("Load data first.")
        else:
            try:
                new_rules = suggest_rules(st.session_state.profile)
                st.session_state.rules.extend(new_rules)

                # Save to DB safely
                try:
                    save_rules(st.session_state.rules)
                except:
                    pass

                st.success(f"Added {len(new_rules)} rules.")
            except Exception as e:
                st.error(f"Rule generation failed: {e}")

    # Display rules
    rules = st.session_state.rules
    if not rules:
        st.info("No rules found.")
    else:
        for i, r in enumerate(rules):
            with st.expander(f"Rule #{i+1} — {r.get('column')}"):
                st.json(r)
                if st.button(f"Delete Rule #{i+1}", key=f"rule_del_{i}"):
                    rules.pop(i)
                    try:
                        save_rules(rules)
                    except:
                        pass
                    st.experimental_rerun()

    # Manual rule add
    st.subheader("Add Custom Rule")
    col_name = st.text_input("Column Name")
    rule_text = st.text_area("Rule Description")
    conf = st.slider("Confidence", 0.0, 1.0, 0.8)

    if st.button("Add Rule"):
        if col_name and rule_text:
            rule = {"column": col_name, "rule": rule_text, "confidence": conf}
            st.session_state.rules.append(rule)

            try:
                save_rules(st.session_state.rules)
            except:
                pass

            st.success("Rule added.")


# ---------------- TAB 4: Reports ----------------
with reports_tab:
    st.subheader("PDF Reports")

    df = st.session_state.df
    profile_df = st.session_state.profile
    dq_score = st.session_state.dq_score
    rules = st.session_state.rules

    if df is None or profile_df is None:
        st.info("Run data quality checks first.")
    else:
        if st.button("Generate PDF Report"):
            try:
                pdf_bytes = generate_pdf_report_bytes(
                    df=df,
                    profile_df=profile_df,
                    dq_score=dq_score,
                    rules=rules,
                )

                # Save metadata safely
                try:
                    report_id = save_report_meta(
                        title="Streamlit DQ Report",
                        dq_score=dq_score,
                        row_count=len(df),
                    )
                except:
                    report_id = 0

                st.success(f"Report ready (ID: {report_id})")
                st.download_button(
                    "Download PDF",
                    pdf_bytes,
                    file_name="dq_report.pdf",
                    mime="application/pdf",
                )
            except Exception as e:
                st.error(f"Report generation failed: {e}")

