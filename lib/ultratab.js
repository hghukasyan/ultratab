"use strict";

const path = require("path");
const fs = require("fs");

const pkgRoot = path.resolve(__dirname, "..");

function loadAddon() {
  const candidates = [
    path.join(pkgRoot, "build", "Release", "ultratab.node"),
    path.join(pkgRoot, "build", "Debug", "ultratab.node"),
  ];

  for (const candidate of candidates) {
    try {
      if (fs.existsSync(candidate)) {
        return require(candidate);
      }
    } catch {
      continue;
    }
  }

  const err = new Error(
    "ultratab: Native addon not found. Run `npm run build` to compile the addon. " +
    "If you installed from npm, ensure CMake and a C++17 compiler are available, then run `npm rebuild ultratab`."
  );
  err.code = "ULTRATAB_ADDON_NOT_FOUND";
  throw err;
}

const addon = loadAddon();

/**
 * Create an async iterable over CSV row batches.
 * @param {string} filePath - Path to the CSV file
 * @param {object} [options] - Parser options
 * @param {string} [options.delimiter=','] - Field delimiter
 * @param {string} [options.quote='"'] - Quote character
 * @param {boolean} [options.headers=false] - If true, skip first row as header
 * @param {number} [options.batchSize=10000] - Rows per batch
 * @returns {AsyncIterable<string[][]>}
 */
function csv(filePath, options) {
  if (typeof filePath !== "string") {
    throw new TypeError("csv(): path must be a string");
  }
  const parser = addon.createParser(filePath, options || {});
  if (!parser) {
    throw new Error("csv(): failed to create parser");
  }

  let destroyed = false;

  function destroy() {
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
          const value = await addon.getNextBatch(parser);
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

/**
 * Create an async iterable over typed columnar CSV batches.
 * @param {string} filePath - Path to the CSV file
 * @param {object} [options] - Parser options
 * @param {string} [options.delimiter=','] - Field delimiter
 * @param {string} [options.quote='"'] - Quote character
 * @param {boolean} [options.headers=true] - If true, first row is header
 * @param {number} [options.batchSize=10000] - Rows per batch
 * @param {string[]} [options.select] - Columns to keep (by header name)
 * @param {Record<string,'string'|'int32'|'int64'|'float64'|'bool'>} [options.schema] - Per-column types
 * @param {string[]} [options.nullValues=['','null','NULL']] - Strings treated as null
 * @param {boolean} [options.trim] - Trim whitespace
 * @param {'string'|'null'} [options.typedFallback='null'] - If parse fails for typed column
 * @returns {AsyncIterable<{headers:string[],columns:Record<string,any>,nullMask?:Record<string,Uint8Array>,rows:number}>}
 */
function csvColumns(filePath, options) {
  if (typeof filePath !== "string") {
    throw new TypeError("csvColumns(): path must be a string");
  }
  const parser = addon.createColumnarParser(filePath, options || {});
  if (!parser) {
    throw new Error("csvColumns(): failed to create parser");
  }

  let destroyed = false;

  function destroy() {
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
          const value = await addon.getNextColumnarBatch(parser);
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

/**
 * Create an async iterable over XLSX row/columnar batches.
 * @param {string} filePath - Path to the .xlsx file
 * @param {object} [options] - Parser options
 * @param {number|string} [options.sheet=1] - 1-based sheet index or sheet name
 * @param {boolean} [options.headers=true] - First row as headers
 * @param {number} [options.batchSize=5000] - Rows per batch
 * @param {string[]} [options.select] - Columns to keep (by header name)
 * @param {Record<string,'string'|'int32'|'int64'|'float64'|'bool'>} [options.schema] - Per-column types
 * @param {string[]} [options.nullValues] - Strings treated as null
 * @param {boolean} [options.trim] - Trim whitespace
 * @param {'string'|'null'} [options.typedFallback='null'] - If parse fails for typed column
 * @returns {AsyncIterable<{headers:string[],rows:string[][]|Record<string,any>,rowsCount:number}>}
 */
function xlsx(filePath, options) {
  if (typeof filePath !== "string") {
    throw new TypeError("xlsx(): path must be a string");
  }
  const parser = addon.createXlsxParser(filePath, options || {});
  if (!parser) {
    throw new Error("xlsx(): failed to create parser");
  }

  let destroyed = false;

  function destroy() {
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
          const value = await addon.getNextXlsxBatch(parser);
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

function getParserMetrics(parser) {
  if (!parser) return null;
  return addon.getParserMetrics ? addon.getParserMetrics(parser) : null;
}

function getColumnarParserMetrics(parser) {
  if (!parser) return null;
  return addon.getColumnarParserMetrics ? addon.getColumnarParserMetrics(parser) : null;
}

module.exports = {
  csv,
  csvColumns,
  xlsx,
  getParserMetrics,
  getColumnarParserMetrics,
  createParser: (path, opts) => addon.createParser(path, opts),
  getNextBatch: (parser) => addon.getNextBatch(parser),
  destroyParser: (parser) => addon.destroyParser(parser),
  createColumnarParser: (path, opts) => addon.createColumnarParser(path, opts),
  getNextColumnarBatch: (parser) => addon.getNextColumnarBatch(parser),
  destroyColumnarParser: (parser) => addon.destroyColumnarParser(parser),
};
