# dqtool/duplicates.py
import pandas as pd
from rapidfuzz import fuzz


def detect_duplicates_simple(
    df: pd.DataFrame,
    subset_cols=None,
    threshold: int = 90,
) -> pd.Series:
    """
    Simple fuzzy duplicate detection:
    - Combines selected columns into a single string key
    - For each row, compares against already seen rows using token_sort_ratio
    - If similarity >= threshold, it is flagged as duplicate

    Returns: boolean Series (True = potential duplicate).
    """
    if subset_cols is None:
        subset_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
        if not subset_cols:
            subset_cols = [df.columns[0]]

    keys = df[subset_cols].astype(str).fillna("")
    combined = keys.apply(
        lambda row: " || ".join([str(x) for x in row.values]),
        axis=1,
    )

    seen = []
    is_dup = []

    for val in combined:
        found = False
        for s in seen:
            if fuzz.token_sort_ratio(val, s) >= threshold:
                found = True
                break
        is_dup.append(found)
        seen.append(val)

    return pd.Series(is_dup, index=df.index)
