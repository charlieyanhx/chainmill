"""The map step."""
import zipfile

import pytest

from chainmill import COLUMNS, extract_archive, session_date_from_name
from chainmill.extract import read_archive


# --- filename dates ---------------------------------------------------------

@pytest.mark.parametrize("name,expected", [
    ("chain_2024-01-02.zip", "2024-01-02"),
    ("SPY20240102.zip", "2024-01-02"),
    ("x_2024_01_02.zip", "2024-01-02"),
    ("no-date-here.zip", None),
    ("chain_2024-13-45.zip", None),        # parses as digits, invalid as a date
])
def test_session_date_from_name(name, expected):
    assert session_date_from_name(name) == expected


# --- happy path -------------------------------------------------------------

def test_extract_returns_normalised_frame(make_archive):
    r = extract_archive(make_archive(n_rows=25))
    assert r.ok and r.rows == 25
    assert list(r.frame.columns) == COLUMNS
    assert (r.frame["date"] == "2024-01-02").all()


def test_multiple_csv_members_are_concatenated(make_archive):
    r = extract_archive(make_archive(n_rows=10, members=3))
    assert r.ok and r.rows == 30


def test_reading_does_not_outlive_the_archive(make_archive):
    """Regression: the reader was created inside the `with` blocks and iterated
    after they closed, so every archive raised "I/O operation on closed file",
    was swallowed by a bare except, and reported zero rows. Silently."""
    frame = read_archive(make_archive(n_rows=12))
    assert len(frame) == 12          # touching the frame after close must work
    assert frame["strike"].sum() > 0


def test_extract_never_raises_on_bad_input(tmp_path):
    """One unreadable file must not abort a 1,400-archive build."""
    junk = tmp_path / "chain_2024-01-02.zip"
    junk.write_bytes(b"this is not a zip")
    r = extract_archive(junk)
    assert r.ok is False and r.rows == 0 and r.frame is None
    assert "BadZipFile" in r.error or "Error" in r.error


# --- failure modes ----------------------------------------------------------

def test_missing_columns_are_reported_not_guessed(make_archive):
    short = ["strike", "bid", "ask"]
    r = extract_archive(make_archive(columns=short))
    assert r.ok is False and "missing columns" in r.error


def test_archive_without_csv_member_fails_cleanly(tmp_path):
    p = tmp_path / "chain_2024-01-02.zip"
    with zipfile.ZipFile(p, "w") as z:
        z.writestr("readme.txt", "nothing here")
    r = extract_archive(p)
    assert r.ok is False and "no CSV member" in r.error


def test_undated_filename_fails_cleanly(make_archive):
    r = extract_archive(make_archive(name="undated.zip"))
    assert r.ok is False and "no date" in r.error


def test_explicit_session_date_overrides_the_filename(make_archive):
    r = extract_archive(make_archive(name="undated.zip"), session_date="1999-12-31")
    assert r.ok and (r.frame["date"] == "1999-12-31").all()


# --- column aliasing --------------------------------------------------------

def test_vendor_aliases_are_accepted(make_archive):
    from chainmill.schema import REQUIRED
    renamed = ["iv" if c == "implied_volatility" else
               "volume" if c == "trade_volume" else
               "expiry" if c == "expiration" else c for c in REQUIRED]
    r = extract_archive(make_archive(columns=renamed))
    assert r.ok, r.error
    assert "implied_volatility" in r.frame.columns and "trade_volume" in r.frame.columns


def test_header_case_and_spacing_are_tolerated(make_archive):
    from chainmill.schema import REQUIRED
    noisy = [c.upper().replace("_", " ") for c in REQUIRED]
    r = extract_archive(make_archive(columns=noisy))
    assert r.ok, r.error
    assert list(r.frame.columns) == COLUMNS
