"""The reduce step: one writer, batched inserts, one durable store.

Workers never touch the database. Every parallel version of this pipeline that
let workers open their own connection either lost rows to lock contention or
persisted nothing at all; the reduce belongs in the parent, on one connection.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

from .schema import COLUMNS, create_index_sql, create_table_sql

logger = logging.getLogger(__name__)

TABLE = "quotes"
_INSERT = f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) VALUES ({', '.join('?' * len(COLUMNS))})"


class QuoteStore:
    """SQLite-backed quote store. Single writer; safe to reopen and extend."""

    def __init__(self, path, batch_size: int = 50_000):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self._conn = sqlite3.connect(str(self.path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._create_schema()

    # -- lifecycle ---------------------------------------------------------

    def _create_schema(self):
        self._conn.execute(create_table_sql(TABLE))
        # Ingested archives, so a rebuild can skip what it already has.
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS ingested ("
            f"  archive TEXT PRIMARY KEY, session_date TEXT, rows INTEGER)"
        )
        self._conn.commit()

    def create_indexes(self):
        """Build indexes. Call after bulk load - indexing during insert is slow."""
        for stmt in create_index_sql(TABLE):
            self._conn.execute(stmt)
        self._conn.commit()

    def close(self):
        if self._conn is not None:
            self._conn.commit()
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- writing -----------------------------------------------------------

    def write_frame(self, frame: pd.DataFrame, archive: Optional[str] = None) -> int:
        """Append a normalised frame. Returns rows written."""
        if frame is None or frame.empty:
            return 0

        ordered = frame[COLUMNS]
        rows = list(ordered.itertuples(index=False, name=None))

        cur = self._conn.cursor()
        for start in range(0, len(rows), self.batch_size):
            cur.executemany(_INSERT, rows[start:start + self.batch_size])

        if archive is not None:
            cur.execute(
                "INSERT OR REPLACE INTO ingested (archive, session_date, rows) VALUES (?, ?, ?)",
                (str(archive), str(ordered["date"].iloc[0]), len(rows)),
            )
        self._conn.commit()
        return len(rows)

    def already_ingested(self) -> set:
        cur = self._conn.execute("SELECT archive FROM ingested")
        return {r[0] for r in cur.fetchall()}

    def forget(self, archive: str) -> int:
        """Remove an archive's rows and its ingest record, so it can be redone."""
        cur = self._conn.cursor()
        cur.execute("SELECT session_date FROM ingested WHERE archive = ?", (str(archive),))
        row = cur.fetchone()
        if row is None:
            return 0
        cur.execute(f"DELETE FROM {TABLE} WHERE date = ?", (row[0],))
        deleted = cur.rowcount
        cur.execute("DELETE FROM ingested WHERE archive = ?", (str(archive),))
        self._conn.commit()
        return deleted

    # -- reading -----------------------------------------------------------

    def row_count(self) -> int:
        return self._conn.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]

    def sessions(self) -> List[str]:
        cur = self._conn.execute(f"SELECT DISTINCT date FROM {TABLE} ORDER BY date")
        return [r[0] for r in cur.fetchall()]

    def coverage(self) -> dict:
        """Rows in, sessions covered, date range - the line every build prints."""
        sessions = self.sessions()
        return {
            "rows": self.row_count(),
            "archives": len(self.already_ingested()),
            "sessions": len(sessions),
            "first_session": sessions[0] if sessions else None,
            "last_session": sessions[-1] if sessions else None,
        }

    def connection(self) -> sqlite3.Connection:
        return self._conn
