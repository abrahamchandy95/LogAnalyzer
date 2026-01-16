"""
Avoids loose strings and makes refactors safer.
"""

RUN = "run"
REQUEST_ID = "request_id"
NODE = "node"
TID = "tid"
TS = "ts"
EVENT = "event"

UDF_MS = "udf_ms"
QUERY_NAME = "query_name"
GRAPH_NAME = "graph_name"
ENDPOINT = "endpoint"

RESTPP_NODE = "restpp_node"
RESTPP_TS = "restpp_ts"
RESTPP_RETURN_TS = "restpp_return_ts"
RESTPP_RETURN_MS = "restpp_return_ms"

GPE_NODE = "gpe_node"
FIRST_SEEN_GPE_TS = "first_seen_gpe_ts"
LAST_SEEN_GPE_TS = "last_seen_gpe_ts"
START_UDF_TS = "start_udf_ts"
STOP_UDF_TS = "stop_udf_ts"
REPORTED_STOP_UDF_MS = "reported_stop_udf_ms"

ACTUAL_STOP_UDF_MS = "actual_stop_udf_ms"
ACTUAL_DIFF_FIRST_LAST_SEEN_MS = "actual_diff_first_last_seen_ms"
DIFF_GPE_DURATION_UDF_MS = "diff_gpe_duration_udf_ms"

FIRST_SEEN_TS_RESTPP = "first_seen_ts_restpp"
SYNC_RETURN_TS_RESTPP = "sync_return_result_ts_restpp"
SYNC_RETURN_TIME_MS = "sync_return_time_ms"
FULL_ENDPOINT = "full_endpoint"
ENDPOINT_NAME = "endpoint_name"

GAP_MS = "gap_ms"
STEP_KEY = "step_key"
ITERATION = "iteration"
PREV_TS = "prev_ts"
PREV_EVENT = "prev_event"
PREV_LABEL = "prev_label"
LABEL = "label"
DETAIL = "detail"

LOG_PATH = "log_path"
LINENO = "lineno"
RAW_MSG = "raw_msg"


# ---- Step stats outputs ----
N = "n"
MEDIAN_MS = "median_ms"
P95_MS = "p95_ms"
MEAN_MS = "mean_ms"
MAX_MS = "max_ms"
SUM_MS = "sum_ms"

# ---- Compare outputs ----
PRESENT_IN = "present_in"

BASE_N = "base_n"
BASE_MEDIAN_MS = "base_median_ms"
BASE_P95_MS = "base_p95_ms"
BASE_MEAN_MS = "base_mean_ms"
BASE_MAX_MS = "base_max_ms"
BASE_SUM_MS = "base_sum_ms"

OPT_N = "opt_n"
OPT_MEDIAN_MS = "opt_median_ms"
OPT_P95_MS = "opt_p95_ms"
OPT_MEAN_MS = "opt_mean_ms"
OPT_MAX_MS = "opt_max_ms"
OPT_SUM_MS = "opt_sum_ms"

OPT_OVER_BASE_MEAN = "opt_over_base_mean"
DIFF_MEAN_MS = "diff_mean_ms"
OPT_OVER_BASE_MEDIAN = "opt_over_base_median"
DIFF_MEDIAN_MS = "diff_median_ms"

POS_IN_REQUEST = "pos_in_request"
MEDIAN_POS = "median_pos"

BASE_POS = "base_pos"
OPT_POS = "opt_pos"
POS = "pos"
SELECT_LIKE_IDX = "select_like_idx"
