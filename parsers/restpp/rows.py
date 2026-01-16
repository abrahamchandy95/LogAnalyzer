from math import nan

import pandas as pd

from parsers._walker import ParsedLine

from .records import RawRequestParsed, ReturnResultParsed, RestppRow


def make_raw_row(*, pl: ParsedLine, parsed: RawRequestParsed) -> RestppRow:
    return {
        "run": pl.run,
        "node": pl.node,
        "ts": pl.ts,
        "tid": pl.tid,
        "log_path": str(pl.log_path),
        "lineno": pl.lineno,
        "request_id": parsed.request_id,
        "method": parsed.method,
        "endpoint": parsed.endpoint,
        "query_name": parsed.query_name,
        "restpp_return_ms": nan,
        "restpp_engine": None,
        "return_ts": pd.NaT,
    }


def make_return_row(*, pl: ParsedLine, parsed: ReturnResultParsed) -> RestppRow:
    return {
        "run": pl.run,
        "node": pl.node,
        "ts": pl.ts,
        "tid": pl.tid,
        "log_path": str(pl.log_path),
        "lineno": pl.lineno,
        "request_id": parsed.request_id,
        "restpp_return_ms": parsed.ms,
        "restpp_engine": parsed.engine,
        "return_ts": pl.ts,
    }
