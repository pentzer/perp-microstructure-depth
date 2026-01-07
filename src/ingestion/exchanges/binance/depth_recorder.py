import asyncio
import time
from pathlib import Path
from typing import Optional

import orjson
import websockets

from src.ingestion.writers.rotating_jsonl_writer import (
    WriteItem,
    writer_loop,
)

# =========================
# CONSTANTS
# =========================

BINANCE_FSTREAM_WS = "wss://fstream.binance.com/ws"

WRITE_QUEUE_MAX = 500_000
ROTATE_GRANULARITY_SEC = 60  # one file per minute


# =========================
# TIME + BUCKETING
# =========================

def now_ns() -> int:
    return time.time_ns()


def minute_bucket(ts_ns: int) -> int:
    return int((ts_ns // 1_000_000_000) // 60)


def minute_filename(bucket: int) -> str:
    return f"deltas_utcmin_{bucket}.jsonl"


# =========================
# VALIDATION
# =========================

def validate_depth_msg(msg: dict) -> None:
    required = ["e", "E", "s", "U", "u", "b", "a"]
    for k in required:
        if k not in msg:
            raise ValueError(f"Missing key {k}")

    if msg["e"] != "depthUpdate":
        raise ValueError(f"Unexpected event type: {msg['e']}")

    if not isinstance(msg["E"], int):
        raise ValueError("E must be int (event time ms)")
    if not isinstance(msg["U"], int) or not isinstance(msg["u"], int):
        raise ValueError("U/u must be int update IDs")
    if not isinstance(msg["b"], list) or not isinstance(msg["a"], list):
        raise ValueError("b/a must be lists")


# =========================
# PUBLIC API
# =========================

async def record_depth(
    *,
    symbol: str,
    out_dir: Path,
    interval_ms: int = 100,
) -> None:
    """
    Record raw Binance futures depth deltas into minute-rotated JSONL files.

    Parameters
    ----------
    symbol : str
        Trading symbol, e.g. "BTCUSDT"
    out_dir : Path
        Directory where raw files will be written
    interval_ms : int
        Depth update interval (Binance supports 100ms)
    """

    stream = f"{symbol.lower()}@depth@{interval_ms}ms"
    url = f"{BINANCE_FSTREAM_WS}/{stream}"

    q: asyncio.Queue = asyncio.Queue(maxsize=WRITE_QUEUE_MAX)

    writer_task = asyncio.create_task(
        writer_loop(
            out_dir=out_dir,
            filename_fn=minute_filename,
            queue=q,
        )
    )

    backoff = 0.25
    max_backoff = 10.0
    conn_id = 0

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=15,
                ping_timeout=10,
                max_queue=8192,
                close_timeout=5,
            ) as ws:
                conn_id += 1
                print(f"[binance] connected (conn_id={conn_id}) {stream}")
                backoff = 0.25

                while True:
                    raw = await ws.recv()
                    recv_ts = now_ns()

                    msg = orjson.loads(raw)
                    validate_depth_msg(msg)

                    record = {
                        "exchange": "binance",
                        "symbol": symbol,
                        "conn_id": conn_id,
                        "recv_ts_ns": recv_ts,
                        "event_ts_ms": msg["E"],
                        "U": msg["U"],
                        "u": msg["u"],
                        "b": msg["b"],
                        "a": msg["a"],
                    }

                    bucket = minute_bucket(recv_ts)
                    line = orjson.dumps(record) + b"\n"

                    try:
                        q.put_nowait(WriteItem(bucket=bucket, line=line))
                    except asyncio.QueueFull:
                        raise RuntimeError(
                            "Writer queue full — disk throughput insufficient; refusing to drop data."
                        )

        except (asyncio.CancelledError, KeyboardInterrupt):
            print("[binance] stopping recorder")
            break
        except Exception as e:
            print(
                f"[binance] error: {type(e).__name__}: {e} "
                f"— reconnecting in {backoff:.2f}s"
            )
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 2)

    # graceful shutdown
    await q.put(None)
    await writer_task


def main() -> None:
    """Legacy entrypoint: record BTCUSDT raw depth deltas into data/binance/BTCUSDT/raw."""
    import asyncio
    from pathlib import Path

    ROOT = Path(__file__).resolve().parents[4]

    asyncio.run(
        record_depth(
            symbol='BTCUSDT',
            out_dir=ROOT / 'data/binance/BTCUSDT/raw',
        )
    )
