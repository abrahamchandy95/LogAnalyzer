import pandas as pd


def stable_dedupe(
    df: pd.DataFrame,
    *,
    sort_cols: list[str],
    subset: list[str],
) -> pd.DataFrame:
    """
    Deterministic dedupe:
      - stable sort so keep='first' is reproducible
      - then drop_duplicates on the provided subset
    """
    if df.empty:
        return df

    df2 = df.sort_values(sort_cols, kind="mergesort", na_position="last")
    return df2.drop_duplicates(subset=subset, keep="first")
