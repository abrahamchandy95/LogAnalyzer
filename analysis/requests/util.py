import pandas as pd


def first_str(s: pd.Series) -> str | None:
    return next((x for x in s if isinstance(x, str)), None)


def elapsed_ms(
    df: pd.DataFrame, *, start_col: str, stop_col: str, out_col: str
) -> pd.DataFrame:
    out = df.copy()
    out[out_col] = (out[stop_col] - out[start_col]).dt.total_seconds() * 1000.0
    return out
