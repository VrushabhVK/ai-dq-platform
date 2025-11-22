import pandas as pd
import numpy as np
def profile_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    cols = []
    for c in df.columns:
        s = df[c]
        cols.append({
            'column': c,
            'dtype': str(s.dtype),
            'null_count': int(s.isnull().sum()),
            'null_pct': float(s.isnull().mean() * 100),
            'distinct_count': int(s.nunique(dropna=True)),
            'distinct_pct': float(s.nunique(dropna=True) / max(1, len(s)) * 100)
        })
    return pd.DataFrame(cols)
