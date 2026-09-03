"""The reduce step and the whole map-reduce build."""
import pytest

from chainmill import ChainQuery, QuoteStore, build, extract_archive


# --- store ------------------------------------------------------------------

def test_write_and_count(tmp_path, make_archive):
    r = extract_archive(make_archive(n_rows=20))
    with QuoteStore(tmp_path / "q.db") as s:
        assert s.write_frame(r.frame, archive=r.path) == 20
        assert s.row_count() == 20


def test_store_reopens_and_extends(tmp_path, make_archive):
    db = tmp_path / "q.db"
    a = extract_archive(make_archive(session="2024-01-02", n_rows=10))
    with QuoteStore(db) as s:
        s.write_frame(a.frame, archive=a.path)
    b = extract_archive(make_archive(session="2024-01-03", n_rows=10))
    with QuoteStore(db) as s:
        s.write_frame(b.frame, archive=b.path)
        assert s.row_count() == 20 and len(s.sessions()) == 2


def test_ingest_ledger_tracks_archives(tmp_path, make_archive):
    r = extract_archive(make_archive())
    with QuoteStore(tmp_path / "q.db") as s:
        s.write_frame(r.frame, archive=r.path)
        assert r.path in s.already_ingested()


def test_forget_removes_rows_and_ledger_entry(tmp_path, make_archive):
    r = extract_archive(make_archive(n_rows=15))
    with QuoteStore(tmp_path / "q.db") as s:
        s.write_frame(r.frame, archive=r.path)
        assert s.forget(r.path) == 15
        assert s.row_count() == 0 and r.path not in s.already_ingested()


def test_empty_frame_writes_nothing(tmp_path):
    with QuoteStore(tmp_path / "q.db") as s:
        assert s.write_frame(None) == 0
        assert s.row_count() == 0


# --- build ------------------------------------------------------------------

def test_build_ingests_every_archive(tmp_path, archive_dir):
    rep = build(archive_dir, tmp_path / "q.db", parallel=False)
    assert rep.archives_seen == 3
    assert rep.archives_ingested == 3
    assert rep.archives_failed == 0
    assert rep.rows_written == 60
    assert rep.coverage["sessions"] == 3


def test_parallel_and_sequential_agree(tmp_path, archive_dir):
    """The map step must be a pure function of its archive - the parallel build
    and the in-process build have to produce byte-identical stores."""
    seq = build(archive_dir, tmp_path / "seq.db", parallel=False)
    par = build(archive_dir, tmp_path / "par.db", parallel=True, max_workers=2)
    assert seq.rows_written == par.rows_written
    assert seq.coverage == par.coverage


def test_resume_skips_what_is_already_ingested(tmp_path, archive_dir):
    db = tmp_path / "q.db"
    build(archive_dir, db, parallel=False)
    again = build(archive_dir, db, parallel=False, resume=True)
    assert again.archives_skipped == 3
    assert again.rows_written == 0
    with QuoteStore(db) as s:
        assert s.row_count() == 60, "resume duplicated rows"


def test_resume_off_reingests(tmp_path, archive_dir):
    db = tmp_path / "q.db"
    build(archive_dir, db, parallel=False)
    again = build(archive_dir, db, parallel=False, resume=False)
    assert again.archives_ingested == 3


def test_one_bad_archive_does_not_abort_the_build(tmp_path, archive_dir):
    (archive_dir / "chain_2024-01-05.zip").write_bytes(b"corrupt")
    rep = build(archive_dir, tmp_path / "q.db", parallel=False)
    assert rep.archives_ingested == 3
    assert rep.archives_failed == 1
    assert len(rep.failures) == 1
    assert rep.rows_written == 60


def test_build_reports_coverage(tmp_path, archive_dir):
    rep = build(archive_dir, tmp_path / "q.db", parallel=False)
    assert rep.coverage["first_session"] == "2024-01-02"
    assert rep.coverage["last_session"] == "2024-01-04"
    assert "3/3 archives ingested" in rep.summary()


def test_empty_source_directory_is_not_an_error(tmp_path):
    empty = tmp_path / "none"
    empty.mkdir()
    rep = build(empty, tmp_path / "q.db", parallel=False)
    assert rep.archives_seen == 0 and rep.rows_written == 0


# --- query ------------------------------------------------------------------

@pytest.fixture
def built(tmp_path, archive_dir):
    db = tmp_path / "q.db"
    build(archive_dir, db, parallel=False)
    return db


def test_chain_returns_one_session(built):
    with ChainQuery(built) as q:
        assert len(q.chain("2024-01-02")) == 20
        assert len(q.chain("2099-01-01")) == 0


def test_chain_narrows_by_symbol_and_type(built):
    with ChainQuery(built) as q:
        assert len(q.chain("2024-01-02", symbol="SPY", option_type="P")) == 20
        assert len(q.chain("2024-01-02", symbol="NOPE")) == 0


def test_strike_and_iv_filters(built):
    with ChainQuery(built) as q:
        assert len(q.strikes("2024-01-02", 400, 404)) == 5
        assert len(q.iv_range("2024-01-02", 0.1, 0.3)) == 20
        assert len(q.iv_range("2024-01-02", 0.9, 1.0)) == 0


def test_liquidity_filter(built):
    with ChainQuery(built) as q:
        assert len(q.liquid("2024-01-02", min_volume=5)) == 20
        assert len(q.liquid("2024-01-02", min_volume=999)) == 0


def test_daily_summary_covers_every_session(built):
    with ChainQuery(built) as q:
        s = q.daily_summary()
        assert len(s) == 3 and s["rows"].sum() == 60


def test_sessions_are_sorted(built):
    with ChainQuery(built) as q:
        assert q.sessions() == ["2024-01-02", "2024-01-03", "2024-01-04"]


# --- parquet export ---------------------------------------------------------

def test_export_to_a_single_parquet_file(tmp_path, built):
    pytest.importorskip("pyarrow")
    import pandas as pd
    out = tmp_path / "chains.parquet"
    with ChainQuery(built) as q:
        written = q.to_parquet(out)
    assert written == [str(out)] and out.exists()
    frame = pd.read_parquet(out)
    assert len(frame) == 60
    assert set(frame["date"]) == {"2024-01-02", "2024-01-03", "2024-01-04"}


def test_export_partitioned_by_session(tmp_path, built):
    pytest.importorskip("pyarrow")
    import pandas as pd
    out = tmp_path / "parts"
    with ChainQuery(built) as q:
        written = q.to_parquet(out, partition_by_session=True)
    assert len(written) == 3
    frame = pd.read_parquet(out)
    assert len(frame) == 60, "partitioned read must round-trip every row"
    # `date` is recovered from the directory name, Hive-style
    assert set(frame["date"].astype(str)) == {"2024-01-02", "2024-01-03", "2024-01-04"}


def test_export_preserves_the_schema(tmp_path, built):
    pytest.importorskip("pyarrow")
    import pandas as pd
    from chainmill import COLUMNS
    out = tmp_path / "chains.parquet"
    with ChainQuery(built) as q:
        q.to_parquet(out)
    assert list(pd.read_parquet(out).columns) == COLUMNS


def test_export_streams_in_batches(tmp_path, built):
    """batch_rows smaller than the store must still write every row - the whole
    point is not loading a multi-year store into memory."""
    pytest.importorskip("pyarrow")
    import pandas as pd
    out = tmp_path / "chains.parquet"
    with QuoteStore(built) as s:
        s.to_parquet(out, batch_rows=7)
    assert len(pd.read_parquet(out)) == 60


def test_export_of_an_empty_store(tmp_path):
    pytest.importorskip("pyarrow")
    import pandas as pd
    out = tmp_path / "empty.parquet"
    with QuoteStore(tmp_path / "empty.db") as s:
        s.to_parquet(out)
    assert pd.read_parquet(out).empty
