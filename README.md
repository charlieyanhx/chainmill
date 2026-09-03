# chainmill

**Zipped option chains in, queryable store out.**

[![tests](https://github.com/charlieyanhx/chainmill/actions/workflows/tests.yml/badge.svg)](https://github.com/charlieyanhx/chainmill/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/chainmill.svg)](https://pypi.org/project/chainmill/)
[![Python](https://img.shields.io/pypi/pyversions/chainmill.svg)](https://pypi.org/project/chainmill/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Parallel ingest of vendor option-chain archives into SQLite, with resumable builds and a coverage
line you can quote. Map: one worker parses one archive. Reduce: the parent folds every frame into
one store, on one connection.

```bash
pip install chainmill
```

## Thirty seconds

```python
from chainmill import build, ChainQuery

if __name__ == "__main__":                      # required on macOS and Windows
    report = build("raw_archives/", "chains.db", max_workers=8)
    print(report.summary())
```

```
60/60 archives ingested (0 skipped, 0 failed) - 120,000 rows, 60 sessions
2024-01-02 to 2024-03-01
```

Then query it:

```python
with ChainQuery("chains.db") as q:
    chain = q.chain("2024-01-02", symbol="SPY", option_type="P")
    wide  = q.strikes("2024-01-02", 400, 460)
    busy  = q.liquid("2024-01-02", min_volume=500)
    panel = q.daily_summary()
```

Re-running skips archives already ingested, so an interrupted multi-year load resumes instead of
duplicating rows.

## What it handles

- **Vendor drift.** Headers matched case-insensitively with aliases (`iv` → `implied_volatility`,
  `volume` → `trade_volume`, `expiry` → `expiration`). A file genuinely missing required columns
  is reported by name, never silently coerced.
- **Bad archives.** A corrupt or dateless file is recorded as a failure in the report; it does not
  abort a 1,400-archive build.
- **Multi-member archives.** All CSV members concatenated.
- **Session dates** parsed from `chain_2024-01-02.zip`, `SPY20240102.zip` or `x_2024_01_02.zip`,
  or passed explicitly.
- **Coverage reporting.** Rows in, archives ingested / skipped / failed, and the session range —
  the line a result should be quoted with.

Throughput on a four-core laptop: 60 archives / 120,000 rows in 2.0s including index build. Real
archives are larger and compression-bound.

## Where this fits

| If you want | Use |
|---|---|
| A market-data platform with feeds | [OpenBB](https://github.com/OpenBB-finance/OpenBB) |
| A general dataframe / warehouse layer | DuckDB, Polars, Parquet |
| To turn a folder of vendor zips into something queryable, correctly | **chainmill** |

SQLite and pandas, nothing else. It is an ingest and lookup layer, not an analytics engine and
not a distributed system. Beyond one machine, export and point a real warehouse at it.

No market data is included — option-chain licences do not permit redistribution.

## Why this exists

This replaces five hand-rolled extractors that accumulated in a private research program over two
years. Before rewriting anything, each was tested against a synthetic archive:

| extractor | rows parsed | rows persisted |
|---|---|---|
| `optimized_extractor` | **0** | 0 |
| `extract_and_index` | 50 | none — map only |
| `robust_extractor` | 50 | 50 |
| `space_efficient_processor` | 50 | **0** |

Four defects, each producing a plausible-looking run:

**The reader outlived its archive.** `pd.read_csv(handle, chunksize=...)` returns a *lazy* reader.
It was created inside `with ZipFile(...)` / `with archive.open(...)` and iterated after both
closed — so every archive raised `I/O operation on closed file`, was swallowed by a bare `except`,
and reported zero rows while printing a line that looked like progress. Here the member is read to
completion inside the block, with a regression test that touches the frame after the archive has
closed.

**Workers were bound methods.** Submitting `self.extract_file` to a `ProcessPoolExecutor` pickles
the entire processor to every worker. Each mutated its own copy of the in-memory indexes; those
mutations died with the process, and the parent then wrote the empty originals to disk. The map
step here is a module-level function of a path, with nothing shared.

**The reduce step was distributed.** Workers each opened their own SQLite connection and inserted
row-by-row in a Python loop — lock contention plus per-row overhead, and the reason one extractor
persisted nothing. One writer, `executemany`, batched transactions, WAL.

**Indexes were built before the load**, so every insert paid for them. `create_indexes()` now runs
once, after.

## Tests

```bash
pip install -e ".[test]"
pytest -q
```

33 tests, no fixtures on disk — archives are synthesised in `tmp_path`. Includes an equivalence
test asserting the parallel and in-process builds produce identical stores, which is what keeps
the map step honest about being a pure function.

## Licence

MIT. See [CHANGELOG.md](CHANGELOG.md) and [CONTRIBUTING.md](CONTRIBUTING.md).
