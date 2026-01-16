import pandas as pd

REQUEST_SUMMARY_COLS = pd.Index(
    [
        "run",
        "request_id",
        "gpe_node",
        "first_seen_gpe_ts",
        "last_seen_gpe_ts",
        "start_udf_ts",
        "stop_udf_ts",
        "reported_stop_udf_ms",
        "actual_stop_udf_ms",
        "actual_diff_first_last_seen_ms",
        "diff_gpe_duration_udf_ms",
        "first_seen_ts_restpp",
        "sync_return_result_ts_restpp",
        "sync_return_time_ms",
        "graph_name",
        "query_name",
        "full_endpoint",
        "restpp_node",
        "endpoint_name",
    ]
)

EXEC_REQUEST_TABLE_COLS = pd.Index(
    [
        "run",
        "request_id",
        "start_udf_ts",
        "stop_udf_ts",
        "actual_stop_udf_ms",
        "reported_stop_udf_ms",
        "query_name",
        "graph_name",
        "endpoint",
    ]
)
