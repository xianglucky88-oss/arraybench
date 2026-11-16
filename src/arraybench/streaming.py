"""Disk-backed staging and residency accounting for streamed append batches.

A logical append batch may be much larger than ``max_batch_rows`` because a
generator is consumed exactly once and normalized rows are spilled to a local
SQLite index.  The heap only ever holds the bounded windows that are actively
being normalized, fetched, merged or written into a chunk.

Nothing in this module publishes: staging directories and content-addressed
chunks are disposable until a snapshot object is referenced by ``HEAD``.
"""
import sqlite3
from pathlib import Path


class ResidencyProbe:
    """Count record-bearing rows currently held in memory.

    ``hold``/``release`` bracket every list or fetch window that keeps live
    records.  ``peak`` is the high-water mark for the current scope; callers
    reset it between ingestion and each bucket merge, which lets tests assert
    the documented bounds independently of wall-clock behaviour.
    """

    def __init__(self):
        self.live = 0
        self.peak = 0
        self.kind_live = {}
        self.kind_peak = {}
        self.holds = 0

    def hold(self, count, kind):
        if count <= 0:
            return
        self.live += count
        self.holds += 1
        self.kind_live[kind] = self.kind_live.get(kind, 0) + count
        self.kind_peak[kind] = max(self.kind_peak.get(kind, 0), self.kind_live[kind])
        if self.live > self.peak:
            self.peak = self.live

    def release(self, count, kind):
        if count <= 0:
            return
        self.live -= count
        self.kind_live[kind] = self.kind_live.get(kind, 0) - count
        if self.live < 0 or self.kind_live[kind] < 0:
            raise AssertionError("unbalanced residency accounting")

    def reset_peak(self):
        self.peak = self.live


class Staging:
    """Spill normalized rows to a throw-away SQLite database.

    Rows are keyed by ``(bucket, t, coordinate)`` plus the original sequence
    number, so a stable external sort recovers both the key order used by
    chunks and the input order needed for duplicate policies.
    """

    def __init__(self, directory, fetch_rows):
        self.directory = Path(directory)
        self.fetch_rows = max(1, int(fetch_rows))
        self.path = self.directory / "rows.sqlite3"
        self.db = sqlite3.connect(self.path)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("PRAGMA temp_store=FILE")
        self.db.execute("PRAGMA cache_size=-2048")  # ~2 MiB page cache, not rows
        self.db.execute(
            "CREATE TABLE rows (bucket TEXT NOT NULL, t INTEGER NOT NULL, "
            "coord TEXT NOT NULL, seq INTEGER NOT NULL, payload BLOB NOT NULL)"
        )
        self._rows = 0
        self._indexed = False

    def add_many(self, records):
        if not records:
            return
        self.db.executemany("INSERT INTO rows VALUES (?, ?, ?, ?, ?)", records)
        self._rows += len(records)

    def finish(self):
        """Seal insertion and build the external ordering index."""
        self.db.commit()
        self.db.execute("CREATE INDEX rows_order ON rows (bucket, t, coord, seq)")
        self.db.commit()
        self._indexed = True

    @property
    def row_count(self):
        return self._rows

    def buckets(self):
        """Buckets in first-input-encounter order, matching one-shot append.

        Yields lazily so a stream that spans very many buckets does not build
        a Python list of bucket identifiers.
        """
        if not self._indexed:
            raise RuntimeError("staging index is not sealed")
        cursor = self.db.execute("SELECT bucket FROM rows GROUP BY bucket ORDER BY MIN(seq)")
        try:
            while True:
                batch = cursor.fetchmany(self.fetch_rows)
                if not batch:
                    return
                for (bucket,) in batch:
                    yield bucket
        finally:
            cursor.close()

    def _stream(self, cursor, probe):
        try:
            while True:
                batch = cursor.fetchmany(self.fetch_rows)
                if not batch:
                    return
                if probe is not None:
                    probe.hold(len(batch), "staged")
                try:
                    for record in batch:
                        yield record
                finally:
                    if probe is not None:
                        probe.release(len(batch), "staged")
        finally:
            cursor.close()

    def key_counts(self, bucket, probe=None):
        """Yield ``(t, coord, count)`` per staged key in chunk key order.

        Used to reject duplicates before touching output chunks.  Grouping in
        SQLite keeps a key that repeats across many blocks from ever being
        materialized together.
        """
        cursor = self.db.execute(
            "SELECT t, coord, COUNT(*) FROM rows WHERE bucket = ? "
            "GROUP BY t, coord ORDER BY t, coord",
            (bucket,),
        )
        yield from self._stream(cursor, probe)

    def winners(self, bucket, newest, probe=None):
        """Yield ``(t, coord, payload)`` once per staged key in key order.

        ``newest`` selects the last occurrence (``update``) and otherwise the
        first (``ignore``/``error``), reproducing one-shot input-order
        semantics while only one record per key is ever held.  The correlated
        aggregate resolves on the ``(bucket, t, coord, seq)`` index, so SQLite
        streams results in key order without materializing a window partition.
        """
        aggregate = "MAX" if newest else "MIN"
        cursor = self.db.execute(
            "SELECT r.t, r.coord, r.payload FROM rows AS r "
            "WHERE r.bucket = ? AND r.seq = ("
            f"  SELECT {aggregate}(s.seq) FROM rows AS s "
            "  WHERE s.bucket = r.bucket AND s.t = r.t AND s.coord = r.coord"
            ") ORDER BY r.t, r.coord",
            (bucket,),
        )
        yield from self._stream(cursor, probe)

    def close(self):
        try:
            self.db.close()
        except sqlite3.Error:
            pass
