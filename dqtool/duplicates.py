# dqtool/duplicates.py
import pandas as pd
from rapidfuzz import fuzz


def detect_duplicates_simple(
    df: pd.DataFrame,
    subset_cols=None,
    threshold: int = 90,
) -> pd.Series:
    """
    Improved duplicate detection:
    - Ignores fully empty rows (prevents none-based duplicates)
    - Uses only columns that have meaningful values
    - Avoids marking rows as duplicates if they don't share any real data
    """

    # 1. Select subset columns
    if subset_cols is None:
        subset_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
        if not subset_cols:
            subset_cols = [df.columns[0]]

    # 2. Convert to strings and fillna
    keys = df[subset_cols].astype(str).fillna("")

    # 3. Ignore rows where all subset values are empty
    non_empty_mask = keys.apply(lambda r: any(val.strip() != "" for val in r), axis=1)

    # Create combined keys only for rows that have real values
    combined = keys[non_empty_mask].apply(
        lambda row: " || ".join([str(x).strip() for x in row.values]),
        axis=1,
    )

    seen = []
    is_dup = pd.Series([False] * len(df), index=df.index)

    # 4. Compare only non-empty rows
    for idx, val in combined.items():
        is_duplicate = False

        for s in seen:
            # Ignore fuzzy matching if both strings are empty or meaningless
            if val.strip() == "" or s.strip() == "":
                continue

            if fuzz.token_sort_ratio(val, s) >= threshold:
                is_duplicate = True
                break

        is_dup.at[idx] = is_duplicate
        seen.append(val)

    return is_dup
