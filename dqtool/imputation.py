# dqtool/imputation.py
import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer


def impute_missing_knn(
    df: pd.DataFrame,
    n_neighbors: int = 3,
) -> pd.DataFrame:
    """
    Apply KNN imputation to numeric columns only.
    Non-numeric columns are left as-is.
    """
    numeric_cols = df.select_dtypes(include=[np.number]).columns

    # If no numeric columns, just return a copy
    if len(numeric_cols) == 0:
        return df.copy()

    imputer = KNNImputer(n_neighbors=n_neighbors)
    arr = imputer.fit_transform(df[numeric_cols])

    df_imputed = df.copy()
    df_imputed[numeric_cols] = arr
    return df_imputed
