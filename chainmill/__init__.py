"""chainmill - parallel extraction of zipped option chains into a queryable store.

    from chainmill import build, ChainQuery

    report = build("raw_archives/", "chains.db", max_workers=8)
    print(report.summary())

    with ChainQuery("chains.db") as q:
        chain = q.chain("2024-01-02", symbol="SPY", option_type="P")

Map: one worker parses one archive to a normalised frame.
Reduce: the parent folds every frame into one SQLite store on one connection.
"""

from .extract import ExtractError, ExtractResult, extract_archive, session_date_from_name
from .mill import BuildReport, build, find_archives
from .query import ChainQuery
from .schema import COLUMNS, REQUIRED, SchemaError, missing_columns, normalise_columns
from .store import QuoteStore

__version__ = "0.1.0"

__all__ = [
    "build",
    "BuildReport",
    "find_archives",
    "ChainQuery",
    "QuoteStore",
    "extract_archive",
    "ExtractResult",
    "ExtractError",
    "session_date_from_name",
    "SchemaError",
    "normalise_columns",
    "missing_columns",
    "COLUMNS",
    "REQUIRED",
    "__version__",
]
