import fcntl
import hashlib
import json
import os
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path


class SnapshotConflict(ValueError):
    """The base snapshot moved before this batch could be published."""


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


class ChunkStore:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        for directory in ("chunks", "snapshots", "staging"):
            (self.root / directory).mkdir(exist_ok=True)

    @contextmanager
    def lock(self):
        with (self.root / ".lock").open("a+b") as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            try: yield
            finally: fcntl.flock(stream, fcntl.LOCK_UN)

    def write(self, path, data):
        fd, temporary = tempfile.mkstemp(prefix=".tmp-", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary): os.unlink(temporary)

    def put(self, category, value):
        content = encode(value)
        sha = hashlib.sha256(content).hexdigest()
        path = self.root / category / (sha + ".json")
        if path.exists():
            if path.read_bytes() != content: raise ValueError("existing object is corrupt")
        else: self.write(path, content)
        return sha

    def get(self, category, sha):
        if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{64}", sha):
            raise ValueError("invalid object identifier")
        data = (self.root / category / (sha + ".json")).read_bytes()
        if hashlib.sha256(data).hexdigest() != sha:
            raise ValueError(f"{category} checksum mismatch: {sha}")
        return json.loads(data)

    def head(self):
        path = self.root / "HEAD"
        return path.read_text().strip() if path.exists() else None

    def commit(self, snapshot):
        sha = self.put("snapshots", snapshot)
        self.write(self.root / "HEAD", sha.encode())
        return sha

    def publish(self, snapshot, base):
        """Commit a prepared snapshot only if HEAD still names ``base``."""
        with self.lock():
            if self.head() != base:
                raise SnapshotConflict("snapshot revision conflict")
            return self.commit(snapshot)

    def publish_object(self, sha, base):
        """Point HEAD at an already written snapshot object if the base matches.

        The object is re-read and checksum-verified before the pointer moves,
        so a truncated or hand-edited snapshot can never become HEAD.
        """
        self.get("snapshots", sha)
        with self.lock():
            if self.head() != base:
                raise SnapshotConflict("snapshot revision conflict")
            self.write(self.root / "HEAD", sha.encode())
        return sha
