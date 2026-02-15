# UltraTab

[![npm version](https://img.shields.io/npm/v/ultratab.svg)](https://www.npmjs.com/package/ultratab)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Node](https://img.shields.io/badge/node-%3E%3D18-brightgreen.svg)](https://nodejs.org/)

Ultra-fast native CSV and XLSX streaming parser for Node.js. Built as a C++ addon with SIMD acceleration, background-thread parsing, and bounded memory for large files (100MB–10GB+).

## Features

- **Streaming**: Parses from disk in chunks; does not load entire files into memory
- **Non-blocking**: Parsing runs on a C++ background thread; the Node event loop stays responsive
- **Two APIs**: Row-based `csv()` (string[][]) and typed columnar `csvColumns()` (TypedArrays)
- **Typed output**: int32, int64, float64, bool → Int32Array, BigInt64Array, Float64Array, Uint8Array
- **XLSX support**: `xlsx()` parses .xlsx files in low-memory streaming mode
- **SIMD acceleration**: AVX2/SSE2 on x86_64 (Linux/Windows); scalar fallback on macOS

## Installation

```bash
npm install ultratab
```

**Requirements:** Node.js >= 18, CMake >= 3.15, C++17 compiler (GCC, Clang, or MSVC)

## Quick Start

After `npm install ultratab`, try the example:

```bash
node node_modules/ultratab/examples/csv-demo.js
```

### CSV (row-based)

```javascript
const { csv } = require("ultratab");

for await (const batch of csv("data.csv", { batchSize: 10000, headers: true })) {
  console.log("Rows in batch:", batch.length);
  for (const row of batch) {
    console.log(row); // string[]
  }
}
```

### CSV (typed columnar)

```javascript
const { csvColumns } = require("ultratab");

for await (const batch of csvColumns("data.csv", {
  headers: true,
  schema: { id: "int32", amount: "float64", active: "bool" },
  select: ["id", "amount", "active"],
})) {
  const ids = batch.columns.id;       // Int32Array
  const amounts = batch.columns.amount; // Float64Array
  const nulls = batch.nullMask?.amount; // 1 = null
  for (let i = 0; i < batch.rows; i++) {
    if (nulls?.[i]) continue;
    console.log(ids[i], amounts[i]);
  }
}
```

### XLSX

```javascript
const { xlsx } = require("ultratab");

for await (const batch of xlsx("data.xlsx", {
  sheet: 1,
  headers: true,
  batchSize: 5000,
})) {
  console.log("Headers:", batch.headers);
  console.log("Rows:", batch.rowsCount);
}
```

## API Reference

### `csv(path, options?)`

Returns `AsyncIterable<string[][]>`. Each batch is an array of rows; each row is `string[]`.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `delimiter` | string | `","` | Field delimiter (use `"\t"` for TSV) |
| `quote` | string | `'"'` | Quote character |
| `headers` | boolean | `false` | Skip first row as header |
| `batchSize` | number | `10000` | Rows per batch (1–10,000,000) |
| `maxQueueBatches` | number | `2` | Max batches in queue (backpressure) |
| `useMmap` | boolean | `false` | Use memory-mapped I/O |
| `readBufferSize` | number | `262144` | Read buffer size in bytes |

### `csvColumns(path, options?)`

Returns `AsyncIterable<ColumnarBatch>`. Each batch has `headers`, `columns`, `nullMask`, and `rows`.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `delimiter` | string | `","` | Field delimiter |
| `quote` | string | `'"'` | Quote character |
| `headers` | boolean | `true` | First row is header |
| `batchSize` | number | `10000` | Rows per batch |
| `select` | string[] | (all) | Columns to keep by header name |
| `schema` | object | (string) | Per-column: `"string"`, `"int32"`, `"int64"`, `"float64"`, `"bool"` |
| `nullValues` | string[] | `["","null","NULL"]` | Strings treated as null |
| `trim` | boolean | `false` | Trim whitespace |
| `typedFallback` | string | `"null"` | On parse failure: `"null"` or `"string"` |

### `xlsx(path, options?)`

Returns `AsyncIterable<XlsxBatchResult>`. Options: `sheet`, `headers`, `batchSize`, `select`, `schema`, `nullValues`, `trim`, `typedFallback`.

## Performance

Designed for large files: minimal allocations, SIMD-accelerated scanning (x86_64), and bounded backpressure. Typical throughput: hundreds of thousands to millions of rows per second depending on schema and hardware.

## Platform Compatibility

- **Linux** (x86_64): Full SIMD (AVX2/SSE2)
- **Windows** (x64): Full SIMD
- **macOS** (Intel/ARM): Scalar path (no SIMD flags; still fast)

## Build from Source

```bash
git clone <repo-url>
cd ultratab
npm install
npm run build
```

**Compiler requirements:**
- Windows: Visual Studio Build Tools
- macOS: Xcode Command Line Tools (`xcode-select --install`)
- Linux: `build-essential` (or equivalent)

## Troubleshooting

**"Native addon not found"**
- Run `npm run build` or `npm rebuild ultratab`
- Ensure CMake and a C++17 compiler are installed

**Build fails on Windows**
- Install [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) with the "Desktop development with C++" workload

**Build fails on Linux**
- `sudo apt-get install cmake build-essential` (Debian/Ubuntu)

## License

MIT
