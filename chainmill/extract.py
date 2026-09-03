"""The map step: one archive in, a normalised frame out.

Deliberately a module-level function taking a path, not a method on a processor.
A bound method submitted to a ProcessPoolExecutor pickles the whole instance to
every worker, and any state the worker accumulates is discarded when it exits --
which is how an earlier version of this pipeline lost its entire reduce step.
"""
from __future__ import annotations

import logging
import re
import zipfile
from dataclasses import dataclass
from datetime import date as _date
from pathlib import Path
from typing import Optional

import pandas as pd

from .schema import COLUMNS, SchemaError, missing_columns, normalise_columns

logger = logging.getLogger(__name__)

_DATE_IN_NAME = re.compile(r"(\d{4})[-_]?(\d{2})[-_]?(\d{2})")


class ExtractError(RuntimeError):
    """An archive could not be turned into rows."""


@dataclass(frozen=True)
class ExtractResult:
    """Outcome of mapping one archive. Always returned -- never raised past the
    pool -- so a single bad file cannot abort a 1,400-file build."""

    path: str
    session_date: Optional[str]
    rows: int
    frame: Optional[pd.DataFrame]
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None


def session_date_from_name(path) -> Optional[str]:
    """Pull the session date out of an archive filename (YYYY-MM-DD)."""
    m = _DATE_IN_NAME.search(Path(path).stem)
    if not m:
        return None
    try:
        return _date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
    except ValueError:
        return None


def read_archive(path) -> pd.DataFrame:
    """Read every CSV member of a zip into one frame.

    The whole member is read inside the `with` block. Returning a lazy reader
    from inside a closed archive raises "I/O operation on closed file" on first
    iteration -- silent when the caller wraps the map step in a bare except.
    """
    path = Path(path)
    frames = []
    with zipfile.ZipFile(path, "r") as archive:
        members = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if not members:
            raise ExtractError(f"no CSV member in {path.name}")
        for member in members:
            with archive.open(member) as handle:
                frames.append(pd.read_csv(handle, low_memory=False))
    return pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]


def extract_archive(path, session_date: Optional[str] = None) -> ExtractResult:
    """Map one archive to a normalised frame. Pure: no shared state, picklable."""
    path = Path(path)
    try:
        session = session_date or session_date_from_name(path)
        if session is None:
            raise ExtractError(f"no date in filename {path.name!r}")

        raw = read_archive(path)

        missing = missing_columns(raw.columns)
        if missing:
            raise SchemaError(f"{path.name} missing columns: {', '.join(missing)}")

        frame = raw.rename(columns=normalise_columns(raw.columns))
        frame = frame[[c for c in COLUMNS if c != "date"]].copy()
        frame.insert(0, "date", session)

        return ExtractResult(str(path), session, len(frame), frame)

    except Exception as e:  # reported, never swallowed
        logger.warning("extract failed for %s: %s", path, e)
        return ExtractResult(str(path), None, 0, None, error=f"{type(e).__name__}: {e}")
