from typing import Any  # Import Any
import pandas as pd


def as_df(x: Any) -> pd.DataFrame:
    if isinstance(x, pd.DataFrame):
        return x
    return pd.DataFrame(x)


def series_or_none(df: pd.DataFrame, col: str) -> pd.Series | None:
    v = df.get(col)
    return v if isinstance(v, pd.Series) else None


def filter_notna(df: pd.DataFrame, col: str) -> pd.DataFrame:
    s = series_or_none(df, col)
    if s is None:
        return df.copy()

    return as_df(df.loc[pd.notna(s)].copy())
