# chainmill

Turn a directory of zipped option-chain archives into a queryable SQLite store, in parallel,
without losing rows to the failure modes that make ad-hoc ETL scripts quietly wrong.

Map: one worker parses one archive into a normalised frame.
Reduce: the parent folds every frame into one store, on one connection.

```bash
pip install chainmill
```

## Use

```python
from chainmill import build, ChainQuery

if __name__ == "__main__":                      # required on macOS/Windows
    report = build("raw_archives/", "chains.db", max_workers=8)
    print(report.summary())
    # 60/60 archives ingested (0 skipped, 0 failed) - 120,000 rows,
    # 60 sessions 2024-01-02 to 2024-03-01

with ChainQuery("chains.db") as q:
    chain = q.chain("2024-01-02", symbol="SPY", option_type="P")
    wide  = q.strikes("2024-01-02", 400, 460)
    busy  = q.liquid("2024-01-02", min_volume=500)
    panel = q.daily_summary()
```

Builds resume: re-running skips archives already recorded as ingested, so an interrupted
multi-year load picks up where it stopped instead of duplicating rows.

## Why it is shaped this way

This replaces five hand-rolled extractors that accumulated in a private research program over
two years. Testing them against a synthetic archive produced this:

| extractor | rows parsed | rows persisted |
|---|---|---|
| `optimized_extractor` | **0** | 0 |
| `extract_and_index` | 50 | none — map only |
| `robust_extractor` | 50 | 50 |
| `space_efficient_processor` | 50 | **0** |

Four defects, each of which produced a plausible-looking run:

**The reader outlived its archive.** `pd.read_csv(handle, chunksize=...)` returns a *lazy*
reader. It was created inside `with ZipFile(...)` / `with archive.open(...)`, then iterated after
both closed — so every archive raised `I/O operation on closed file`, was swallowed by a bare
`except`, and reported zero rows while printing a line that looked like progress. Here the member
is read to completion inside the block, and there is a regression test that touches the frame
after the archive has closed.

**Workers were bound methods.** Submitting `self.extract_file` to a `ProcessPoolExecutor` pickles
the entire processor to every worker. Each worker mutated its own copy of the in-memory indexes;
those mutations died with the process, and the parent then wrote the empty originals to disk. The
map step here is a module-level function of a path, with nothing shared.

**The reduce step was distributed.** Workers each opened their own SQLite connection and inserted
row-by-row with `cursor.execute` in a Python loop. That is lock contention plus per-row overhead,
and it is why one extractor persisted nothing. One writer, `executemany`, batched transactions,
WAL.

**Indexes were built before the load.** Indexing during insert makes every insert pay for it.
`create_indexes()` runs once, after.

## What it handles

- **Vendor drift.** Headers are matched case-insensitively with aliases (`iv` →
  `implied_volatility`, `volume` → `trade_volume`, `expiry` → `expiration`). A file genuinely
  missing required columns is reported by name, not silently coerced.
- **Bad archives.** A corrupt or dateless file is recorded as a failure in the report; it does not
  abort a 1,400-archive build.
- **Multi-member archives.** All CSV members are concatenated.
- **Session dates** parsed from `chain_2024-01-02.zip`, `SPY20240102.zip`, or `x_2024_01_02.zip`,
  or passed explicitly.
- **Coverage reporting.** Every build returns rows in, archives ingested, skipped and failed, and
  the session range — the coverage line a result should be quoted with.

## Scope

SQLite and pandas, nothing else. It is an ingest and lookup layer, not an analytics engine and not
a distributed system; for anything beyond one machine, point a real warehouse at the parquet you
export. The schema is the standard end-of-day option-chain shape (23 columns: quote identity,
OHLCV, bid/ask, underlying bid/ask, IV, and the five Greeks).

Throughput on a 4-core laptop: 60 archives / 120,000 rows in 2.0s (~60k rows/s), including index
build. Real archives are larger and compression-bound.

No market data is included. Bring your own; the licences on option-chain history do not permit
redistribution.

## Tests

```bash
pip install -e ".[test]"
pytest -q
```

33 tests, no fixtures on disk — archives are synthesised in `tmp_path`. The suite includes an
equivalence test asserting the parallel and in-process builds produce identical stores, which is
what keeps the map step honest about being a pure function.

## Licence

MIT.
