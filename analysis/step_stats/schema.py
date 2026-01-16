import pandas as pd

from analysis import dfkeys as K

STEP_STATS_COLS = pd.Index(
    [
        K.QUERY_NAME,
        K.STEP_KEY,
        K.ITERATION,
        K.N,
        K.MEDIAN_MS,
        K.P95_MS,
        K.MEAN_MS,
        K.MAX_MS,
        K.SUM_MS,
    ]
)

COMPARE_COLS = pd.Index(
    [
        K.STEP_KEY,
        K.ITERATION,
        K.PRESENT_IN,
        K.BASE_N,
        K.BASE_MEDIAN_MS,
        K.BASE_P95_MS,
        K.BASE_MEAN_MS,
        K.BASE_MAX_MS,
        K.BASE_SUM_MS,
        K.OPT_N,
        K.OPT_MEDIAN_MS,
        K.OPT_P95_MS,
        K.OPT_MEAN_MS,
        K.OPT_MAX_MS,
        K.OPT_SUM_MS,
        K.OPT_OVER_BASE_MEAN,
        K.DIFF_MEAN_MS,
        K.OPT_OVER_BASE_MEDIAN,
        K.DIFF_MEDIAN_MS,
    ]
)
