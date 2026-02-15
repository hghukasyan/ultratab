/**
 * Options for the streaming CSV parser.
 */
export interface CsvOptions {
  /** Field delimiter (default: ","). Use "\t" for TSV. */
  delimiter?: string;
  /** Quote character (default: '"'). */
  quote?: string;
  /** If true, skip the first row as header (default: false). */
  headers?: boolean;
  /** Number of rows per batch (default: 10000). Range: 1–10,000,000. */
  batchSize?: number;
  /** Max batches in producer-consumer queue; controls backpressure (default: 2). */
  maxQueueBatches?: number;
  /** Use memory-mapped I/O instead of buffered read (default: false). */
  useMmap?: boolean;
  /** Read buffer size in bytes when not using mmap (default: 262144). */
  readBufferSize?: number;
}

/**
 * Async iterable of row batches. Each batch is an array of rows;
 * each row is an array of field strings.
 */
export type CsvRowBatch = string[][];

/**
 * Options for the columnar CSV parser.
 */
export interface CsvColumnsOptions {
  /** Field delimiter (default: ","). Use "\t" for TSV. */
  delimiter?: string;
  /** Quote character (default: '"'). */
  quote?: string;
  /** If true, first row is header (default: true). */
  headers?: boolean;
  /** Rows per batch (default: 10000). */
  batchSize?: number;
  /** Optional list of columns to keep (by header name). */
  select?: string[];
  /** Per-column schema: "string" | "int32" | "int64" | "float64" | "bool". */
  schema?: Record<string, "string" | "int32" | "int64" | "float64" | "bool">;
  /** Strings treated as null (default: ["", "null", "NULL"]). */
  nullValues?: string[];
  /** Trim whitespace. */
  trim?: boolean;
  /** If parse fails for typed field: "string" | "null" (default: "null"). */
  typedFallback?: "string" | "null";
}

/**
 * Columnar batch: SoA layout with TypedArrays.
 */
export interface ColumnarBatch {
  headers: string[];
  columns: Record<
    string,
    | string[]
    | Int32Array
    | BigInt64Array
    | Float64Array
    | Uint8Array
  >;
  /** 1 = null at that row index (for typed columns). */
  nullMask?: Record<string, Uint8Array>;
  rows: number;
}

/**
 * Streaming columnar CSV parser. Returns typed columnar batches.
 * Parsing runs on a background thread; SIMD-accelerated on x86_64 (Linux/Windows).
 *
 * @param path - Path to the CSV file
 * @param options - Parser options
 * @returns AsyncIterable of columnar batches
 *
 * @example
 * ```ts
 * import { csvColumns } from "ultratab";
 *
 * for await (const batch of csvColumns("data.csv", {
 *   schema: { id: "int32", amount: "float64" },
 *   select: ["id", "amount", "name"]
 * })) {
 *   const ids = batch.columns.id as Int32Array;
 *   const amounts = batch.columns.amount as Float64Array;
 *   const nulls = batch.nullMask?.amount;
 *   for (let i = 0; i < batch.rows; i++) {
 *     if (nulls?.[i]) continue;
 *     console.log(ids[i], amounts[i]);
 *   }
 * }
 * ```
 */
export function csvColumns(
  path: string,
  options?: CsvColumnsOptions
): AsyncIterable<ColumnarBatch>;

/**
 * Streaming CSV parser. Returns an async iterable over row batches.
 * Parsing runs on a background thread; the Node event loop is not blocked.
 *
 * @param path - Path to the CSV file
 * @param options - Parser options
 * @returns AsyncIterable of row batches (string[][])
 *
 * @example
 * ```ts
 * import { csv } from "ultratab";
 *
 * for await (const batch of csv("data.csv", { batchSize: 10000, headers: true })) {
 *   console.log("Rows in batch:", batch.length);
 *   for (const row of batch) {
 *     console.log(row);
 *   }
 * }
 * ```
 */
export function csv(
  path: string,
  options?: CsvOptions
): AsyncIterable<CsvRowBatch>;

/**
 * Options for the streaming XLSX parser.
 */
export interface XlsxOptions {
  /** Sheet to read: 1-based index (default 1) or sheet name. */
  sheet?: number | string;
  /** First row as headers (default true). */
  headers?: boolean;
  /** Rows per batch (default 5000). */
  batchSize?: number;
  /** Optional columns to keep (by header name). */
  select?: string[];
  /** Per-column schema: "string" | "int32" | "int64" | "float64" | "bool". */
  schema?: Record<string, "string" | "int32" | "int64" | "float64" | "bool">;
  /** Strings treated as null. */
  nullValues?: string[];
  /** Trim whitespace. */
  trim?: boolean;
  /** If parse fails for typed field: "string" | "null". */
  typedFallback?: "string" | "null";
}

/**
 * XLSX batch: headers, rows (array of arrays or columnar record), and count.
 */
export type XlsxBatchResult = {
  headers: string[];
  rows:
    | string[][]
    | Record<
        string,
        | string[]
        | Int32Array
        | BigInt64Array
        | Float64Array
        | Uint8Array
      >;
  rowsCount: number;
  nullMask?: Record<string, Uint8Array>;
};

/**
 * Streaming XLSX parser. Returns an async iterable over row or columnar batches.
 * Parsing runs on a background thread; low-memory streaming (ZIP + SAX-style XML).
 *
 * @param path - Path to the .xlsx file
 * @param options - Parser options
 * @returns AsyncIterable of batches with headers, rows, rowsCount
 */
export function xlsx(
  path: string,
  options?: XlsxOptions
): AsyncIterable<XlsxBatchResult>;

/**
 * Low-level CSV parser API. Returns a parser handle for manual batch iteration.
 * Remember to call destroyParser when done.
 */
export function createParser(path: string, options?: CsvOptions): unknown;

/** Get the next row batch from a parser. Returns undefined when done. */
export function getNextBatch(parser: unknown): Promise<string[][] | undefined>;

/** Release parser resources. Call after iteration completes or on early exit. */
export function destroyParser(parser: unknown): void;

/** Internal pipeline metrics (bytes_read, rows_parsed, batches_emitted, etc.). */
export function getParserMetrics(parser: unknown): Record<string, number> | null;

/**
 * Low-level columnar CSV parser API. Returns a parser handle.
 * Remember to call destroyColumnarParser when done.
 */
export function createColumnarParser(
  path: string,
  options?: CsvColumnsOptions
): unknown;

/** Get the next columnar batch. Returns undefined when done. */
export function getNextColumnarBatch(parser: unknown): Promise<ColumnarBatch | undefined>;

/** Release columnar parser resources. */
export function destroyColumnarParser(parser: unknown): void;

/** Internal metrics for columnar parser. */
export function getColumnarParserMetrics(parser: unknown): Record<string, number> | null;
