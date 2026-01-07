import json
import time
from dataclasses import dataclass
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import orjson

# =========================
# CONFIG (SCHEMA-LEVEL)
# =========================

PRICE_SCALE = 10**8
QTY_SCALE = 10**8

getcontext().prec = 50


# =========================
# FIXED-POINT HELPERS
# =========================

def to_fp(x: str, scale: int) -> int:
    return int((Decimal(x) * scale).to_integral_exact())


def normalize_level(level: list) -> Tuple[int, int]:
    return to_fp(level[0], PRICE_SCALE), to_fp(level[1], QTY_SCALE)


# =========================
# SCHEMA VALIDATION
# =========================

def is_depth_delta(raw: dict) -> bool:
    required = (
        "exchange",
        "symbol",
        "conn_id",
        "recv_ts_ns",
        "event_ts_ms",
        "U",
        "u",
        "b",
        "a",
    )
    for k in required:
        if k not in raw:
            return False
    return True


def normalize_delta(raw: dict) -> dict:
    return {
        "exchange": raw["exchange"],
        "symbol": raw["symbol"],
        "conn_id": raw["conn_id"],
        "recv_ts_ns": raw["recv_ts_ns"],
        "event_ts_ns": raw["event_ts_ms"] * 1_000_000,
        "U": raw["U"],
        "u": raw["u"],
        "b": [normalize_level(l) for l in raw["b"]],
        "a": [normalize_level(l) for l in raw["a"]],
    }


# =========================
# ORDER BOOK (TOP ONLY)
# =========================

class TopBook:
    def __init__(self):
        self.bids: Dict[int, int] = {}
        self.asks: Dict[int, int] = {}

    def apply(self, d: dict) -> None:
        for p, q in d["b"]:
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q

        for p, q in d["a"]:
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q

    def best(self) -> Optional[Tuple[int, int, int, int]]:
        if not self.bids or not self.asks:
            return None
        best_bid = max(self.bids)
        best_ask = min(self.asks)
        return best_bid, self.bids[best_bid], best_ask, self.asks[best_ask]


def mid_fp(bid: int, ask: int) -> int:
    return (bid + ask) // 2


def micro_fp(bid: int, bid_sz: int, ask: int, ask_sz: int) -> int:
    den = bid_sz + ask_sz
    if den <= 0:
        return mid_fp(bid, ask)
    num = ask * bid_sz + bid * ask_sz
    return num // den


# =========================
# AUDIT
# =========================

@dataclass
class Audit:
    raw_lines: int = 0
    kept_lines: int = 0
    bad_json: int = 0
    skipped_schema: int = 0
    normalize_errors: int = 0
    continuity_ok: bool = True
    gaps: int = 0
    first_gap: Optional[dict] = None


def continuity_ok(prev_u: Optional[int], U: int, u: int) -> bool:
    if prev_u is None:
        return True
    return U <= prev_u + 1 <= u


# =========================
# CORE PROCESSOR
# =========================

def list_raw_files(raw_dir: Path) -> List[Path]:
    return sorted(p for p in raw_dir.glob("deltas_utcmin_*.jsonl") if p.is_file())


def process_raw_dir(raw_dir: Path) -> None:
    raw_dir = raw_dir.resolve()
    base = raw_dir.parent

    norm_dir = base / "normalized"
    prices_dir = base / "prices"
    audit_dir = base / "audit"

    norm_dir.mkdir(parents=True, exist_ok=True)
    prices_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    for raw_file in list_raw_files(raw_dir):
        stem = raw_file.stem

        norm_file = norm_dir / f"{stem}.fp.jsonl"
        prices_file = prices_dir / f"{stem}.prices.jsonl"
        audit_file = audit_dir / f"{stem}.audit.json"

        if norm_file.exists() and prices_file.exists() and audit_file.exists():
            continue

        audit = Audit()
        book = TopBook()
        prev_u: Optional[int] = None

        with (
            raw_file.open("rb") as fin,
            norm_file.open("wb") as fnorm,
            prices_file.open("wb") as fpx,
        ):
            for line in fin:
                audit.raw_lines += 1

                try:
                    raw = orjson.loads(line)
                except Exception:
                    audit.bad_json += 1
                    continue

                if not is_depth_delta(raw):
                    audit.skipped_schema += 1
                    continue

                try:
                    d = normalize_delta(raw)
                except Exception:
                    audit.normalize_errors += 1
                    continue

                audit.kept_lines += 1

                U, u = d["U"], d["u"]
                if not continuity_ok(prev_u, U, u):
                    audit.continuity_ok = False
                    audit.gaps += 1
                    if audit.first_gap is None:
                        audit.first_gap = {
                            "prev_u": prev_u,
                            "U": U,
                            "u": u,
                            "line": audit.raw_lines,
                        }
                    prev_u = None
                else:
                    prev_u = u

                fnorm.write(orjson.dumps(d) + b"\n")

                book.apply(d)
                best = book.best()
                if best is None:
                    continue

                bid, bid_sz, ask, ask_sz = best
                px = {
                    "exchange": d["exchange"],
                    "symbol": d["symbol"],
                    "recv_ts_ns": d["recv_ts_ns"],
                    "event_ts_ns": d["event_ts_ns"],
                    "best_bid": bid,
                    "bid_sz": bid_sz,
                    "best_ask": ask,
                    "ask_sz": ask_sz,
                    "mid": mid_fp(bid, ask),
                    "micro": micro_fp(bid, bid_sz, ask, ask_sz),
                    "price_scale": PRICE_SCALE,
                    "qty_scale": QTY_SCALE,
                }
                fpx.write(orjson.dumps(px) + b"\n")

        with audit_file.open("w", encoding="utf-8") as fa:
            json.dump(
                {
                    "raw_file": str(raw_file),
                    "normalized_file": str(norm_file),
                    "prices_file": str(prices_file),
                    "created_at_unix": int(time.time()),
                    "stats": audit.__dict__,
                },
                fa,
                indent=2,
            )

def discover_and_process(base_data_dir: Path) -> None:
    """
    Discover and normalize all raw depth directories under:

        base_data_dir/<exchange>/<symbol>/raw
    """
    base_data_dir = base_data_dir.resolve()

    for exchange_dir in base_data_dir.iterdir():
        if not exchange_dir.is_dir():
            continue

        for symbol_dir in exchange_dir.iterdir():
            if not symbol_dir.is_dir():
                continue

            raw_dir = symbol_dir / "raw"
            if not raw_dir.exists():
                continue

            print(f"[normalize] {exchange_dir.name}/{symbol_dir.name}")
            process_raw_dir(raw_dir)


def main() -> None:
    """Legacy entrypoint: normalize data/binance/BTCUSDT/raw in-place."""
    from pathlib import Path

    ROOT = Path(__file__).resolve().parents[2]
    process_raw_dir(Path(ROOT / 'data/binance/BTCUSDT/raw'))
