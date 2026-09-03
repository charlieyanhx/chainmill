import csv
import io
import zipfile

import pytest

from chainmill.schema import REQUIRED

_VALUES = {
    "underlying_symbol": "SPY", "quote_datetime": "09:31:00", "root": "SPY",
    "expiration": "2024-02-16", "option_type": "P", "open": 1.0, "high": 1.2,
    "low": 0.8, "close": 1.0, "trade_volume": 10, "bid": 0.9, "ask": 1.1,
    "underlying_bid": 399.0, "underlying_ask": 401.0, "implied_volatility": 0.18,
    "delta": -0.30, "gamma": 0.01, "theta": -0.05, "vega": 0.20, "rho": 0.01,
    "open_interest": 100,
}


def _csv_bytes(n_rows, columns=None, **overrides):
    columns = columns or list(REQUIRED)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(columns)
    for i in range(n_rows):
        row = dict(_VALUES)
        row["strike"] = 400 + i
        row.update(overrides)
        w.writerow([row.get(c, "") for c in columns])
    return buf.getvalue()


@pytest.fixture
def make_archive(tmp_path):
    """Write a zipped chain archive; returns its path."""
    def _make(session="2024-01-02", n_rows=25, columns=None, name=None,
              members=1, empty=False, **overrides):
        path = tmp_path / (name or f"chain_{session}.zip")
        with zipfile.ZipFile(path, "w") as z:
            if not empty:
                for m in range(members):
                    z.writestr(f"chain_{session}_{m}.csv",
                               _csv_bytes(n_rows, columns, **overrides))
        return path
    return _make


@pytest.fixture
def archive_dir(tmp_path, make_archive):
    """A directory of three sessions."""
    d = tmp_path / "raw"
    d.mkdir()
    for session in ("2024-01-02", "2024-01-03", "2024-01-04"):
        make_archive(session=session, n_rows=20).rename(d / f"chain_{session}.zip")
    return d
