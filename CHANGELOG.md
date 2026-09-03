# Changelog

## 0.1.0

First release. A rewrite of the map-reduce core behind five hand-rolled extractors,
only one of which worked end to end.

### Added
- `build()` — parallel map-reduce ingest of zipped chain archives into SQLite.
- `ChainQuery` — indexed lookups by session, symbol, expiration, strike, IV and volume.
- `QuoteStore` — single-writer store with an ingest ledger, so builds resume.
- `BuildReport` — rows in, archives ingested/skipped/failed, and the session range.
- Vendor header aliasing and per-archive failure isolation.

### Fixed
Defects carried by the extractors this replaces:
- **The reader outlived its archive.** `pd.read_csv(handle, chunksize=...)` is lazy; it
  was built inside the `ZipFile`/member `with` blocks and iterated after both closed, so
  every archive raised `I/O operation on closed file`, was swallowed by a bare `except`,
  and reported zero rows while printing what looked like progress.
- **Workers were bound methods.** Submitting `self.extract_file` to a `ProcessPoolExecutor`
  pickles the whole processor per worker; index mutations died with the process.
- **The reduce step was distributed.** Workers each opened their own SQLite connection and
  inserted row by row. Now one writer, `executemany`, batched transactions, WAL.
- **Indexes were built before the load** rather than after.
