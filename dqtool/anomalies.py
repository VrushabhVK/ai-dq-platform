# dqtool/anomalies.py
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest


def detect_anomalies_numeric(
    df: pd.DataFrame,
    contamination: float = 0.01,
) -> pd.Series:
    """
    Return a boolean Series of same index as df:
    True = anomaly row
    """
    num_cols = df.select_dtypes(include=[np.number]).columns

    # If no numeric columns, return a Series of False
    if len(num_cols) == 0:
        return pd.Series(False, index=df.index)

    # Fill numeric NaNs with median before fitting
    X = df[num_cols].copy()
    X = X.fillna(X.median())

    iso = IsolationForest(
        contamination=contamination,
        random_state=42,
    )
    preds = iso.fit_predict(X)

    # IsolationForest: -1 = anomaly, 1 = normal
    return pd.Series(preds == -1, index=df.index)
