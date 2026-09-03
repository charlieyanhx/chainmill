"""Canonical option-chain schema and row normalisation."""
from __future__ import annotations

from typing import Dict, List

# Columns persisted for every quote. Order is the INSERT order; keep them aligned.
COLUMNS: List[str] = [
    "date",
    "underlying_symbol",
    "quote_datetime",
    "root",
    "expiration",
    "strike",
    "option_type",
    "open",
    "high",
    "low",
    "close",
    "trade_volume",
    "bid",
    "ask",
    "underlying_bid",
    "underlying_ask",
    "implied_volatility",
    "delta",
    "gamma",
    "theta",
    "vega",
    "rho",
    "open_interest",
]

# Columns a source file must supply; `date` is derived from the archive name.
REQUIRED: List[str] = [c for c in COLUMNS if c != "date"]

TEXT_COLUMNS = {"date", "underlying_symbol", "quote_datetime", "root", "expiration", "option_type"}
INT_COLUMNS = {"trade_volume", "open_interest"}

SQL_TYPES: Dict[str, str] = {
    c: ("TEXT" if c in TEXT_COLUMNS else "INTEGER" if c in INT_COLUMNS else "REAL")
    for c in COLUMNS
}

# Vendors disagree on spelling. Map their names onto ours.
ALIASES: Dict[str, str] = {
    "underlying": "underlying_symbol",
    "symbol": "underlying_symbol",
    "quote_date": "quote_datetime",
    "expiry": "expiration",
    "expire_date": "expiration",
    "type": "option_type",
    "right": "option_type",
    "volume": "trade_volume",
    "iv": "implied_volatility",
    "oi": "open_interest",
}


class SchemaError(ValueError):
    """A source file does not carry the columns the store needs."""


def normalise_columns(columns) -> Dict[str, str]:
    """Map a source file's column names onto canonical ones.

    Matching is case-insensitive and ignores surrounding whitespace, because
    vendor headers are not stable across years of the same product.
    """
    mapping = {}
    for raw in columns:
        key = str(raw).strip().lower().replace(" ", "_")
        canonical = ALIASES.get(key, key)
        if canonical in COLUMNS:
            mapping[raw] = canonical
    return mapping


def missing_columns(columns) -> List[str]:
    present = set(normalise_columns(columns).values())
    return [c for c in REQUIRED if c not in present]


def create_table_sql(table: str = "quotes") -> str:
    cols = ",\n    ".join(f"{c} {SQL_TYPES[c]}" for c in COLUMNS)
    return f"CREATE TABLE IF NOT EXISTS {table} (\n    {cols}\n)"


def create_index_sql(table: str = "quotes") -> List[str]:
    indexed = ["date", "underlying_symbol", "expiration", "strike", "option_type"]
    return [
        f"CREATE INDEX IF NOT EXISTS idx_{table}_{c} ON {table}({c})" for c in indexed
    ] + [
        # The query that actually runs in research: one chain, one day.
        f"CREATE INDEX IF NOT EXISTS idx_{table}_chain "
        f"ON {table}(date, underlying_symbol, expiration)"
    ]
