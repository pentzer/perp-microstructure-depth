# Market Depth — Perpetual Futures Microstructure Research

This repository contains **infrastructure-grade code** for recording, normalizing, and analyzing
high-frequency Level-2 (order book) market data for perpetual futures markets.

This project is designed to be developed under strict
task delegation. Code correctness and behavioral stability are higher priority than refactoring or elegance.

---

## AUTHORITATIVE PRINCIPLES (READ FIRST)

1. **Behavioral stability is sacred**
   - No refactor may change output formats, filenames, or directory layout unless explicitly instructed.

2. **Separation of concerns**
   - `ingestion/` records raw data
   - `pipelines/` transform data
   - `scripts/` are *thin entrypoints only*

3. **No architectural improvisation**
   - This repo evolves only via explicit task instructions.

4. **Absolute imports only**
   - All Python imports assume repo root is on `PYTHONPATH`.

---

## DIRECTORY OVERVIEW

```text
src/
  ingestion/        # Raw data recorders (WebSocket, APIs)
  pipelines/        # Deterministic data transformations
  utils/            # Shared helpers (if any)
scripts/            # Thin execution wrappers (no logic)
data/               # Runtime-generated datasets (gitignored)
