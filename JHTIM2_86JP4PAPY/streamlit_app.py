import sys
import os
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
