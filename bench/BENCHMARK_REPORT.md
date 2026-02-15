# Ultratab vs PapaParse Benchmark Report

This report compares **ultratab** with **PapaParse** on large CSV parsing. Run the benchmarks yourself with the commands below.

## Requirements

- Build the addon: `npm run build`
- Generate datasets: `npm run bench:generate` (optional: `LARGE=1` for 1GB)
- Node.js ≥ 14

## Running Benchmarks

```bash
# CSV benchmarks (ultratab, PapaParse, csv-parse, fast-csv)
npm run bench:csv

# Filter by size/variant for faster runs
SIZE=medium VARIANT=simple npm run bench:csv
```

Results are written to `bench/reports/<timestamp>.json` and `bench/reports/<timestamp>.md`.

## Profiling Ultratab

With `ULTRATAB_PROFILE=1`, the parser records per-stage timings and allocation counts. Use the low-level API and `getParserMetrics` to print them:

```bash
ULTRATAB_PROFILE=1 node demo_profile.js [path-to-csv]
```

Example output:

```
--- Parser metrics ---
bytes_read: ...
rows_parsed: ...
--- Per-stage (when ULTRATAB_PROFILE=1) ---
read_time_ns: ...
build_time_ns: ...
emit_time_ns: ...
arena_resizes: ...
batch_allocations: ...
--- Stages (ms) ---
read: ... parse: ... build: ... emit: ...
```

You can also compile with the define: `ULTRATAB_PROFILE=1 node-gyp rebuild` (or set in binding.gyp) so profiling is always on.

## Expected Outcome: Ultratab vs PapaParse

On large CSV files (e.g. 100MB–1GB), ultratab is designed to:

1. **Throughput (MB/s, rows/s)** – Be faster than PapaParse due to:
   - C++ native parsing and SIMD delimiter/newline scan
   - Background thread (non-blocking event loop)
   - Batched output and minimal per-field allocations (slice + arena)
   - Column selection (skip unneeded columns in slice parser and columnar path)

2. **Memory (Peak RSS)** – Use less memory than PapaParse when:
   - Streaming with bounded queue (default 2 batches)
   - Column selection reduces arena and row size
   - No full-file string in memory (chunked read or mmap)

3. **Event loop** – Block the main thread less because parsing runs on a worker thread.

## Scenarios to Compare

Run at least these three (after `npm run bench:generate`):

| Scenario        | Size   | Variant     | Description              |
|----------------|--------|-------------|---------------------------|
| Medium simple  | 100 MB | simple      | No quotes, minimal work   |
| Medium wide    | 100 MB | wide        | 100 columns               |
| Medium numeric | 100 MB | numeric_heavy | Mostly numbers, schema  |

Example:

```bash
SIZE=medium VARIANT=simple npm run bench:csv
SIZE=medium VARIANT=wide npm run bench:csv
SIZE=medium VARIANT=numeric_heavy npm run bench:csv
```

Compare the reported **Median (ms)**, **MB/s**, **rows/s**, and **Peak RSS (MB)** for `ultratab (string batches)` and `papaparse (stream)`.

## PapaParse Parity

- **Delimiter/quote**: Same behaviour as PapaParse: configurable delimiter and quote; `""` inside quoted fields is one literal quote.
- **Correctness**: See `test/papaparse_parity.test.js` and `test/fuzz_csv.test.js` for parity and fuzz tests against PapaParse.

## Optimizations Implemented

1. **Column selection** – Slice parser skips copying and storing unselected columns when `setSelectedColumnIndices` is used (e.g. from `csvColumns` with `select`).
2. **Defer string creation** – Row output builds strings only for the slice batch; columnar path builds strings only for selected/typed columns as needed.
3. **Fast numeric parsers** – `parseInt32`, `parseInt64`, `parseFloat64`, `parseBool` in `columnar_parser` for schema columns (no locale).
4. **State machine** – Common path (unquoted fields) uses SIMD `scanForSeparator`; quoted path uses `scanForChar` for quote.
5. **SIMD** – Delimiter/newline scan when not in quotes (AVX2/SSE2 on x86_64 where available).
6. **Profiling** – `ULTRATAB_PROFILE=1` or compile define for read/parse/build/emit timings and arena/batch allocation counts.
