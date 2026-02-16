"use strict";

const path = require("path");
const fs = require("fs");

const pkgRoot = path.resolve(__dirname, "..");

function loadAddon(): Record<string, unknown> {
  const candidates = [
    path.join(pkgRoot, "build", "Release", "ultratab.node"),
    path.join(pkgRoot, "build", "Debug", "ultratab.node"),
  ];

  for (const candidate of candidates) {
    try {
      if (fs.existsSync(candidate)) {
        return require(candidate) as Record<string, unknown>;
      }
    } catch {
      continue;
    }
  }

  const err = new Error(
    "ultratab: Native addon not found. Run `npm run build` to compile the addon. " +
    "If you installed from npm, ensure CMake and a C++17 compiler are available, then run `npm rebuild ultratab`."
  );
  (err as NodeJS.ErrnoException).code = "ULTRATAB_ADDON_NOT_FOUND";
  throw err;
}

const addon = loadAddon() as Record<string, (...args: unknown[]) => unknown>;

interface CsvOptions {
  delimiter?: string;
  quote?: string;
  headers?: boolean;
  batchSize?: number;
  maxQueueBatches?: number;
  useMmap?: boolean;
  readBufferSize?: number;
}

interface CsvColumnsOptions {
  delimiter?: string;
  quote?: string;
  headers?: boolean;
  batchSize?: number;
  select?: string[];
  schema?: Record<string, "string" | "int32" | "int64" | "float64" | "bool">;
  nullValues?: string[];
  trim?: boolean;
  typedFallback?: "string" | "null";
}

interface XlsxOptions {
  sheet?: number | string;
  headers?: boolean;
  batchSize?: number;
  select?: string[];
  schema?: Record<string, "string" | "int32" | "int64" | "float64" | "bool">;
  nullValues?: string[];
  trim?: boolean;
  typedFallback?: "string" | "null";
}

function csv(filePath: string, options?: CsvOptions): AsyncIterable<string[][]> {
  if (typeof filePath !== "string") {
    throw new TypeError("csv(): path must be a string");
  }
  const parser = addon.createParser(filePath, options || {});
  if (!parser) {
    throw new Error("csv(): failed to create parser");
  }

  let destroyed = false;

  function destroy(): void {
    if (destroyed) return;
    destroyed = true;
    addon.destroyParser(parser);
  }

  return {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          if (destroyed) {
            return { value: undefined, done: true };
          }
          const value = await addon.getNextBatch(parser) as string[][] | undefined;
          if (value === undefined) {
            destroy();
            return { value: undefined, done: true };
          }
          return { value, done: false };
        },
        async return() {
          destroy();
          return { value: undefined, done: true };
        },
      };
    },
  };
}

function csvColumns(filePath: string, options?: CsvColumnsOptions): AsyncIterable<{
  headers: string[];
  columns: Record<string, string[] | Int32Array | BigInt64Array | Float64Array | Uint8Array>;
  nullMask?: Record<string, Uint8Array>;
  rows: number;
}> {
  if (typeof filePath !== "string") {
    throw new TypeError("csvColumns(): path must be a string");
  }
  const parser = addon.createColumnarParser(filePath, options || {});
  if (!parser) {
    throw new Error("csvColumns(): failed to create parser");
  }

  let destroyed = false;

  function destroy(): void {
    if (destroyed) return;
    destroyed = true;
    addon.destroyColumnarParser(parser);
  }

  return {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          if (destroyed) {
            return { value: undefined, done: true };
          }
          const value = await addon.getNextColumnarBatch(parser) as { headers: string[]; columns: Record<string, string[] | Int32Array | BigInt64Array | Float64Array | Uint8Array>; nullMask?: Record<string, Uint8Array>; rows: number } | undefined;
          if (value === undefined) {
            destroy();
            return { value: undefined, done: true };
          }
          return { value, done: false };
        },
        async return() {
          destroy();
          return { value: undefined, done: true };
        },
      };
    },
  };
}

function xlsx(filePath: string, options?: XlsxOptions): AsyncIterable<{
  headers: string[];
  rows: string[][] | Record<string, string[] | Int32Array | BigInt64Array | Float64Array | Uint8Array>;
  rowsCount: number;
  nullMask?: Record<string, Uint8Array>;
}> {
  if (typeof filePath !== "string") {
    throw new TypeError("xlsx(): path must be a string");
  }
  const parser = addon.createXlsxParser(filePath, options || {});
  if (!parser) {
    throw new Error("xlsx(): failed to create parser");
  }

  let destroyed = false;

  function destroy(): void {
    if (destroyed) return;
    destroyed = true;
    addon.destroyXlsxParser(parser);
  }

  return {
    [Symbol.asyncIterator]() {
      return {
        async next() {
          if (destroyed) {
            return { value: undefined, done: true };
          }
          const value = await addon.getNextXlsxBatch(parser) as { headers: string[]; rows: string[][] | Record<string, string[] | Int32Array | BigInt64Array | Float64Array | Uint8Array>; rowsCount: number; nullMask?: Record<string, Uint8Array> } | undefined;
          if (value === undefined) {
            destroy();
            return { value: undefined, done: true };
          }
          return { value, done: false };
        },
        async return() {
          destroy();
          return { value: undefined, done: true };
        },
      };
    },
  };
}

function getParserMetrics(parser: unknown): Record<string, number> | null {
  if (!parser) return null;
  return (addon.getParserMetrics as ((p: unknown) => Record<string, number> | null))?.(parser) ?? null;
}

function getColumnarParserMetrics(parser: unknown): Record<string, number> | null {
  if (!parser) return null;
  return (addon.getColumnarParserMetrics as ((p: unknown) => Record<string, number> | null))?.(parser) ?? null;
}

module.exports = {
  csv,
  csvColumns,
  xlsx,
  getParserMetrics,
  getColumnarParserMetrics,
  createParser: (p: string, opts?: CsvOptions) => addon.createParser(p, opts),
  getNextBatch: (parser: unknown) => addon.getNextBatch(parser),
  destroyParser: (parser: unknown) => addon.destroyParser(parser),
  createColumnarParser: (p: string, opts?: CsvColumnsOptions) => addon.createColumnarParser(p, opts),
  getNextColumnarBatch: (parser: unknown) => addon.getNextColumnarBatch(parser),
  destroyColumnarParser: (parser: unknown) => addon.destroyColumnarParser(parser),
};
