# dqtool/scoring.py
import pandas as pd


def compute_dq_score(profile_df: pd.DataFrame) -> float:
    """
    Compute a simple Data Quality score (0–100) based on:
    - completeness
    - uniqueness
    - numeric validity (very rough proxy)
    - consistency (string columns)
    """
    df = profile_df.copy()

    # Completeness: 1 - null_pct
    df["completeness"] = 1 - (df["null_pct"] / 100.0)
    completeness = df["completeness"].mean()

    # Uniqueness: average distinct_pct
    uniqueness = (df["distinct_pct"].mean()) / 100.0

    # Numeric validity proxy: do we have numeric columns with distinct values?
    numeric = df[df["dtype"].str.contains("int|float|number", case=False, regex=True)]
    numeric_validity = 1.0 if numeric.empty else numeric["distinct_pct"].notnull().mean()

    # Consistency proxy: same for non-numeric (string-like) columns
    string_cols = df[~df["dtype"].str.contains("int|float|number", case=False, regex=True)]
    consistency = 1.0 if string_cols.empty else string_cols["distinct_pct"].notnull().mean()

    # Weights
    w_completeness = 0.35
    w_uniqueness = 0.25
    w_validity = 0.20
    w_consistency = 0.20

    score = (
        w_completeness * completeness
        + w_uniqueness * uniqueness
        + w_validity * numeric_validity
        + w_consistency * consistency
    )
    return round(score * 100, 2)
