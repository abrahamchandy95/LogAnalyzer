import pandas as pd


def attach_steps_to_requests(gpe_events: pd.DataFrame) -> pd.DataFrame:
    if gpe_events.empty:
        return gpe_events.copy()

    gpe = (
        gpe_events.copy()
        .set_index(["run", "node", "tid", "ts"])
        .sort_index()
        .reset_index()
    )

    gpe["request_id_attached"] = gpe["request_id"]

    for key, sub_idx in gpe.groupby(["run", "node", "tid"]).groups.items():
        # narrow the key just to satisfy typing; we don't use it
        if not isinstance(key, tuple) or len(key) != 3:
            continue

        active_rid: str | None = None
        for i in sub_idx:
            ev = gpe.at[i, "event"]
            rid = gpe.at[i, "request_id_attached"]

            if ev == "UDF_START":
                if isinstance(rid, str) and rid.strip():
                    active_rid = rid.strip()

            elif ev == "UDF_STOP":
                if (not isinstance(rid, str) or not rid.strip()) and isinstance(
                    active_rid, str
                ):
                    gpe.at[i, "request_id_attached"] = active_rid
                active_rid = None

            elif ev == "STEP":
                if (not isinstance(rid, str) or not rid.strip()) and isinstance(
                    active_rid, str
                ):
                    gpe.at[i, "request_id_attached"] = active_rid

    gpe["request_id"] = gpe["request_id_attached"]
    return gpe.drop(columns=["request_id_attached"])
