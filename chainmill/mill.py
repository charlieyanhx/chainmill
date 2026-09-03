"""Map-reduce build: fan archives out to workers, fold results into one store."""
from __future__ import annotations

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional, Sequence

from .extract import ExtractResult, extract_archive
from .store import QuoteStore

logger = logging.getLogger(__name__)


@dataclass
class BuildReport:
    """What the build did. Printed as the coverage line; also asserted in tests."""

    archives_seen: int = 0
    archives_ingested: int = 0
    archives_skipped: int = 0
    archives_failed: int = 0
    rows_written: int = 0
    failures: List[tuple] = field(default_factory=list)
    coverage: dict = field(default_factory=dict)

    def summary(self) -> str:
        return (
            f"{self.archives_ingested}/{self.archives_seen} archives ingested "
            f"({self.archives_skipped} skipped, {self.archives_failed} failed) - "
            f"{self.rows_written:,} rows, {self.coverage.get('sessions', 0)} sessions "
            f"{self.coverage.get('first_session')} to {self.coverage.get('last_session')}"
        )


def find_archives(source_dir, pattern: str = "*.zip") -> List[Path]:
    return sorted(Path(source_dir).glob(pattern))


def build(
    source_dir,
    db_path,
    max_workers: int = 4,
    pattern: str = "*.zip",
    resume: bool = True,
    parallel: bool = True,
    progress: Optional[Callable[[BuildReport], None]] = None,
) -> BuildReport:
    """Extract every archive under `source_dir` into the store at `db_path`.

    Args:
        max_workers: worker processes for the map step.
        resume: skip archives already recorded as ingested.
        parallel: run the map step in-process when False (useful in tests and
            when a traceback matters more than throughput).

    Returns a BuildReport. Individual archive failures are collected, not raised:
    one unreadable file in a multi-year archive must not abort the build.
    """
    archives = find_archives(source_dir, pattern)
    report = BuildReport(archives_seen=len(archives))

    with QuoteStore(db_path) as store:
        done = store.already_ingested() if resume else set()
        todo = [p for p in archives if str(p) not in done]
        report.archives_skipped = len(archives) - len(todo)

        def fold(result: ExtractResult):
            if not result.ok:
                report.archives_failed += 1
                report.failures.append((result.path, result.error))
                return
            report.rows_written += store.write_frame(result.frame, archive=result.path)
            report.archives_ingested += 1
            if progress:
                progress(report)

        if parallel and len(todo) > 1:
            # Workers map only. The single reduce happens here, in the parent.
            try:
                with ProcessPoolExecutor(max_workers=max_workers) as pool:
                    futures = {pool.submit(extract_archive, str(p)): p for p in todo}
                    for future in as_completed(futures):
                        fold(future.result())
            except BrokenProcessPool as e:
                raise RuntimeError(
                    "The worker pool died before any archive was mapped. On macOS and "
                    "Windows, Python spawns workers by re-importing the calling module, "
                    "so a build() call at module scope re-runs itself in every worker. "
                    "Put the call behind `if __name__ == \"__main__\":`, or pass "
                    "parallel=False to map in-process."
                ) from e
        else:
            for path in todo:
                fold(extract_archive(str(path)))

        store.create_indexes()
        report.coverage = store.coverage()

    logger.info(report.summary())
    return report
