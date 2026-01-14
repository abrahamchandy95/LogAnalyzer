import pandas as pd
import numpy as np


def assign_request_ids_by_window(df: pd.DataFrame) -> pd.DataFrame:
    """
    Attaches request_ids to STEP events based on the active window between
    UDF_START and UDF_STOP per thread.
    """
    if df.empty:
        return df

    work_df = df.sort_values(["run", "node", "tid", "ts", "lineno"]).copy()

    # GroupBy.ffill() returns a Series with the same index
    work_df["filled_id"] = work_df.groupby(["run", "node", "tid"])["request_id"].ffill()

    work_df["boundary_state"] = np.nan

    mask_start = work_df["event"] == "UDF_START"
    mask_stop = work_df["event"] == "UDF_STOP"

    work_df.loc[mask_start, "boundary_state"] = "START"
    work_df.loc[mask_stop, "boundary_state"] = "STOP"

    work_df["last_boundary"] = work_df.groupby(["run", "node", "tid"])[
        "boundary_state"
    ].ffill()

    is_start_block = work_df["last_boundary"] == "START"
    valid_mask = is_start_block | mask_stop

    work_df["final_request_id"] = work_df["filled_id"].where(valid_mask, other=np.nan)

    # If the row originally had a request_id, keep it.
    work_df["request_id"] = work_df["request_id"].fillna(work_df["final_request_id"])

    return work_df.drop(
        columns=["filled_id", "boundary_state", "last_boundary", "final_request_id"]
    )
