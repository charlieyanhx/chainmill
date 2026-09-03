"""Read side: indexed lookups over a built store."""
from __future__ import annotations

from typing import List, Optional, Sequence

import pandas as pd

from .store import TABLE, QuoteStore


class ChainQuery:
    """Query a built store. Read-only; safe to open alongside a finished build."""

    def __init__(self, db_path):
        self._store = QuoteStore(db_path)
        self._conn = self._store.connection()

    def close(self):
        self._store.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _frame(self, sql: str, params: Sequence = ()) -> pd.DataFrame:
        return pd.read_sql_query(sql, self._conn, params=tuple(params))

    def sessions(self) -> List[str]:
        return self._store.sessions()

    def coverage(self) -> dict:
        return self._store.coverage()

    def chain(
        self,
        session_date: str,
        symbol: Optional[str] = None,
        expiration: Optional[str] = None,
        option_type: Optional[str] = None,
    ) -> pd.DataFrame:
        """One day's chain, optionally narrowed. Hits the composite index."""
        sql = f"SELECT * FROM {TABLE} WHERE date = ?"
        params: List = [session_date]
        for column, value in (
            ("underlying_symbol", symbol),
            ("expiration", expiration),
            ("option_type", option_type),
        ):
            if value is not None:
                sql += f" AND {column} = ?"
                params.append(value)
        return self._frame(sql, params)

    def strikes(self, session_date: str, low: float, high: float) -> pd.DataFrame:
        return self._frame(
            f"SELECT * FROM {TABLE} WHERE date = ? AND strike BETWEEN ? AND ?",
            (session_date, low, high),
        )

    def iv_range(self, session_date: str, low: float, high: float) -> pd.DataFrame:
        return self._frame(
            f"SELECT * FROM {TABLE} WHERE date = ? "
            f"AND implied_volatility BETWEEN ? AND ?",
            (session_date, low, high),
        )

    def liquid(self, session_date: str, min_volume: int) -> pd.DataFrame:
        return self._frame(
            f"SELECT * FROM {TABLE} WHERE date = ? AND trade_volume >= ? "
            f"ORDER BY trade_volume DESC",
            (session_date, min_volume),
        )

    def to_parquet(self, path, partition_by_session: bool = False) -> List[str]:
        """Export the store as Parquet. See QuoteStore.to_parquet."""
        return self._store.to_parquet(path, partition_by_session=partition_by_session)

    def daily_summary(self) -> pd.DataFrame:
        """Rows, contracts and total volume per session - the shape of the panel."""
        return self._frame(
            f"SELECT date, COUNT(*) AS rows, "
            f"COUNT(DISTINCT expiration) AS expirations, "
            f"SUM(trade_volume) AS volume "
            f"FROM {TABLE} GROUP BY date ORDER BY date"
        )
