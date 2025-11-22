import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import streamlit as st
import pandas as pd
from dqtool.profiling import profile_dataframe
from dqtool.anomalies import detect_anomalies_numeric
from dqtool.duplicates import detect_duplicates_simple
from dqtool.imputation import impute_missing_knn
from dqtool.scoring import compute_dq_score
from dqtool.llm import suggest_rules

st.set_page_config(page_title='AI DQ Platform', layout='wide')
st.title('AI Data Quality Platform — POC')

uploaded = st.file_uploader('Upload CSV/Excel', type=['csv', 'xlsx', 'xls'])
if uploaded:
    try:
        df = pd.read_csv(uploaded)
    except Exception:
        uploaded.seek(0)
        df = pd.read_excel(uploaded)

    st.subheader("Preview")
    st.dataframe(df.head(100))

    profile_df = profile_dataframe(df)
    st.subheader("Profiling")
    st.dataframe(profile_df)

    score = compute_dq_score(profile_df)
    st.metric('DQ Score', f"{score} / 100")

    st.subheader('LLM Suggested Rules')
    rules = suggest_rules(profile_df)
    st.json(rules)

    st.subheader('Anomalies')
    anom = detect_anomalies_numeric(df)
    if anom.any():
        st.dataframe(df[anom])

    st.subheader('Duplicates')
    dup = detect_duplicates_simple(df)
    if dup.any():
        st.dataframe(df[dup])

    if st.button('Run KNN Imputation'):
        imputed = impute_missing_knn(df)
        st.subheader("Imputed sample")
        st.dataframe(imputed.head(20))
