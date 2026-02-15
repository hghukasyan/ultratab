# Ultratab Benchmark Suite

Reproducible benchmarks comparing **ultratab** to popular JavaScript CSV and XLSX parsers. Metrics include throughput (MB/s, rows/s), peak RSS memory, CPU usage, event-loop latency, and streaming vs non-streaming behavior.

---

## Quick Start

```bash
# Install dependencies (includes benchmark deps: papaparse, csv-parse, fast-csv, exceljs)
npm install

# Generate datasets (small 5MB, medium 100MB; optional large 1GB with LARGE=1)
npm run bench:generate

# Run CSV benchmarks only
npm run bench:csv

# Run XLSX benchmarks only
npm run bench:xlsx

# Run all (CSV + XLSX)
npm run bench:all
```

Results are printed to the console and written to:

- `bench/reports/<timestamp>.json`
- `bench/reports/<timestamp>.md`

---

## Benchmark Targets

### CSV

| Parser | Mode | Notes |
|--------|------|------|
| **papaparse** | Stream | `Papa.parse(stream, { step, complete })` |
| **csv-parse** | Stream | Node transform stream, pipe from `fs.createReadStream` |
| **fast-csv** | Stream | `parseStream(readStream)` |
| **ultratab** | String batches | `csv(path, { batchSize })` – async iterable of `string[][]` |
| **ultratab** | Columnar typed | `csvColumns(path, { schema, batchSize })` – async iterable of columnar batches |

### XLSX

| Parser | Mode | Notes |
|--------|------|------|
| **xlsx (SheetJS)** | Full read | Loads entire workbook into memory; **non-streaming** |
| **exceljs** | Stream | `ExcelJS.stream.xlsx.WorkbookReader(stream)` when available |
| **ultratab** | Stream | `xlsx(path, { batchSize })` – async iterable of row/columnar batches |

---

## Datasets

Generated under `bench/data/`.

### Sizes

- **small**: 5 MB (default)
- **medium**: 100 MB (default)
- **large**: 1 GB – only when `LARGE=1` (e.g. `LARGE=1 npm run bench:generate`)

### CSV Variants

- **simple** – No quotes, minimal escaping
- **quoted** – Many quoted fields
- **multiline** – Quoted fields containing newlines
- **wide** – 100 columns
- **numeric_heavy** – Mostly numbers
- **string_heavy** – Mostly strings
- **missing** – Empty/missing values

### XLSX

- One file per size: `xlsx_small.xlsx`, `xlsx_medium.xlsx`, optionally `xlsx_large.xlsx`
- ~25k rows (small), ~500k rows (medium), ~5M rows (large)
- 10 columns, mixed types (string, number, float, boolean)

---

## Metrics Explained

| Metric | Description |
|--------|-------------|
| **Median (ms)** | Median wall-clock parse time over 5 iterations (after 2 warmup runs). |
| **P95 (ms)** | 95th percentile of parse time. |
| **MB/s** | Throughput: file size in MB ÷ median time in seconds. |
| **rows/s** | Rows parsed per second (median). |
| **Peak RSS (MB)** | Peak resident set size during parse (sampled every 50 ms). |
| **Event loop p95 (ms)** | 95th percentile event loop delay (via `perf_hooks.monitorEventLoopDelay`). Higher values mean more main-thread blocking. |
| **Streaming** | Whether the parser streams (yes) or loads the file fully (no). |

CPU time (user/system) is collected with `process.cpuUsage()` and included in the JSON report.

---

## Fairness Rules

- **Same dataset** – Each parser runs on the same file for a given size/variant.
- **File generation excluded** – Datasets are pre-generated; parse time does not include generation.
- **Streaming vs non-streaming** – Reported in the “Streaming” column. SheetJS is full read; others use streams where supported.
- **Batch size** – Streaming parsers use the same logical batch size (default 10,000 rows) where applicable; ultratab’s `batchSize` and csv-parse/fast-csv consumption are aligned for comparable workload.
- **Same columns** – For typed/columnar runs (e.g. ultratab columnar), the same columns are parsed; wide variant uses 100 columns for all.

---

## Optional Filters (Faster Runs)

Run a subset of datasets via environment variables:

```bash
# CSV: only small size, only "simple" variant
SIZE=small VARIANT=simple npm run bench:csv

# XLSX: only small
SIZE=small npm run bench:xlsx

# All: only medium CSV simple + medium XLSX
SIZE=medium VARIANT=simple npm run bench:all
```

---

## Interpreting Results

- **Throughput (MB/s, rows/s)** – Higher is better. Ultratab’s C++ backend and (where used) SIMD and background threading typically yield higher throughput.
- **Peak RSS** – Lower is better for memory-constrained environments. Streaming parsers usually keep RSS closer to file size or less; full-read parsers can spike with file size.
- **Event loop p95** – Lower is better for responsiveness. Parsers that run on a worker or native thread (e.g. ultratab) tend to block the event loop less.
- **Streaming** – Prefer streaming when handling large files or when you need low memory and non-blocking behavior.

---

## Directory Layout

```
bench/
├── config.js              # Sizes, iterations, warmup, paths
├── lib/
│   ├── metrics.js         # measureRun, median/p95, summarizeRuns
│   └── run-bench.js       # runBenchmark (warmup + timed iterations)
├── dataset/
│   ├── generate-csv.js    # CSV dataset generator (all variants)
│   ├── generate-xlsx.js   # XLSX dataset generator
│   └── generate-all.js    # Runs both generators
├── runners/
│   ├── csv-runner.js      # papaparse, csv-parse, fast-csv, ultratab (csv + csvColumns)
│   └── xlsx-runner.js     # xlsx, exceljs, ultratab xlsx
├── reporters/
│   ├── console.js         # console.table output
│   ├── json.js            # JSON report writer
│   └── markdown.js        # Markdown report writer
├── run-csv.js             # Entry: npm run bench:csv
├── run-xlsx.js            # Entry: npm run bench:xlsx
├── run-all.js             # Entry: npm run bench:all
├── data/                  # Generated datasets (gitignored)
└── reports/               # Timestamped JSON and Markdown reports
```

---

## Requirements

- **Node.js** ≥ 14 (for `perf_hooks.monitorEventLoopDelay`, async iterables).
- **Build** – Run `npm run build` (or `npm install`) so the ultratab native addon is built before benchmarking.
