import numpy as np
import pandas as pd


def pct(s: pd.Series, p: float) -> float:
    """
    NaN-safe percentile helper.
    """
    x = s.dropna().to_numpy()
    return float(np.nanpercentile(x, p)) if len(x) else float("nan")
