import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


# =========================
# CONFIG
# =========================

BATCH_SIZE = 2000
FLUSH_INTERVAL_SEC = 0.5


# =========================
# DATA MODEL
# =========================

@dataclass
class WriteItem:
    bucket: int
    line: bytes


# =========================
# WRITER
# =========================

class RotatingJSONLWriter:
    """
    One file per UTC time bucket.
    Uses .tmp files + atomic rename to ensure finalized outputs.
    """

    def __init__(self, out_dir: Path, *, filename_fn):
        """
        Parameters
        ----------
        out_dir : Path
            Directory to write files into
        filename_fn : callable
            Function mapping bucket -> filename
        """
        self.out_dir = out_dir
        self.filename_fn = filename_fn
        self.current_bucket: Optional[int] = None
        self.fp = None
        self.tmp_path: Optional[Path] = None
        self.final_path: Optional[Path] = None
        self.buffer = []
        self.last_flush = time.time()

    def _open_for_bucket(self, bucket: int) -> None:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        name = self.filename_fn(bucket)
        self.final_path = self.out_dir / name
        self.tmp_path = self.out_dir / f"{name}.tmp"
        self.fp = self.tmp_path.open("ab", buffering=1024 * 1024)
        self.current_bucket = bucket

    def _close_and_finalize(self) -> None:
        if self.fp is None:
            return

        if self.buffer:
            self.fp.write(b"".join(self.buffer))
            self.buffer.clear()

        self.fp.flush()
        self.fp.close()
        self.tmp_path.replace(self.final_path)

        self.fp = None
        self.current_bucket = None
        self.tmp_path = None
        self.final_path = None

    def write(self, item: WriteItem) -> None:
        if self.current_bucket is None or item.bucket != self.current_bucket:
            self._close_and_finalize()
            self._open_for_bucket(item.bucket)

        self.buffer.append(item.line)

        now = time.time()
        if len(self.buffer) >= BATCH_SIZE or (now - self.last_flush) >= FLUSH_INTERVAL_SEC:
            self.fp.write(b"".join(self.buffer))
            self.fp.flush()
            self.buffer.clear()
            self.last_flush = now

    def close(self) -> None:
        self._close_and_finalize()


# =========================
# ASYNC LOOP
# =========================

async def writer_loop(
    *,
    out_dir: Path,
    filename_fn,
    queue: asyncio.Queue,
) -> None:
    writer = RotatingJSONLWriter(out_dir, filename_fn=filename_fn)
    try:
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break
            writer.write(item)
            queue.task_done()
    finally:
        writer.close()
