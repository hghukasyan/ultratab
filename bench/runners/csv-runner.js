"use strict";

const fs = require("fs");
const path = require("path");
const { csv, csvColumns } = require("../../index.js");
const Papa = require("papaparse");
const { parse } = require("csv-parse");
const { parseStream } = require("@fast-csv/parse");
const { runBenchmark } = require("../lib/run-bench");
const config = require("../config");

const BATCH_SIZE = config.DEFAULT_BATCH_SIZE;

function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (ch) => chunks.push(ch));
    stream.on("end", () => resolve(Buffer.concat(chunks)));
    stream.on("error", reject);
  });
}

/**
 * PapaParse: streaming via stream + step. Note: Papa can also do sync from string; we use stream for fairness.
 */
async function runPapaParse(filePath, fileSize) {
  return runBenchmark(
    "papaparse (stream)",
    async () => {
      return new Promise((resolve, reject) => {
        let rowCount = 0;
        const stream = fs.createReadStream(filePath, { encoding: "utf8" });
        Papa.parse(stream, {
          header: true,
          step: (results) => {
            rowCount += results.data.length;
          },
          complete: () => resolve({ rowCount, bytesProcessed: fileSize }),
          error: reject,
        });
      });
    },
    { streaming: true }
  );
}

/**
 * csv-parse: Node stream API, pipe from read stream.
 */
async function runCsvParse(filePath, fileSize) {
  return runBenchmark(
    "csv-parse (stream)",
    async () => {
      return new Promise((resolve, reject) => {
        let rowCount = 0;
        const parser = parse({ delimiter: ",", relax_quotes: true });
        let first = true;
        fs.createReadStream(filePath)
          .pipe(parser)
          .on("data", () => {
            if (first) { first = false; return; }
            rowCount++;
          })
          .on("end", () => resolve({ rowCount, bytesProcessed: fileSize }))
          .on("error", reject);
      });
    },
    { streaming: true }
  );
}

/**
 * fast-csv: parseStream from read stream.
 */
async function runFastCsv(filePath, fileSize) {
  return runBenchmark(
    "fast-csv (stream)",
    async () => {
      return new Promise((resolve, reject) => {
        let rowCount = 0;
        const stream = fs.createReadStream(filePath);
        let first = true;
        parseStream(stream, { headers: false })
          .on("data", () => {
            if (first) { first = false; return; }
            rowCount++;
          })
          .on("end", () => resolve({ rowCount, bytesProcessed: fileSize }))
          .on("error", reject);
      });
    },
    { streaming: true }
  );
}

/**
 * ultratab: csv() string row batches.
 */
async function runUltratabCsv(filePath, fileSize) {
  return runBenchmark(
    "ultratab (string batches)",
    async () => {
      let rowCount = 0;
      for await (const batch of csv(filePath, { headers: false, batchSize: BATCH_SIZE })) {
        rowCount += batch.length;
      }
      return { rowCount, bytesProcessed: fileSize };
    },
    { streaming: true }
  );
}

/**
 * ultratab: csvColumns() typed columnar batches. Schema matches typical benchmark cols (col0..col9).
 */
async function runUltratabColumnar(filePath, fileSize, numCols = 10) {
  const schema = {};
  for (let i = 0; i < numCols; i++) {
    schema[`col${i}`] = i % 3 === 0 ? "string" : "float64";
  }
  return runBenchmark(
    "ultratab (columnar typed)",
    async () => {
      let rowCount = 0;
      for await (const batch of csvColumns(filePath, {
        headers: true,
        batchSize: BATCH_SIZE,
        schema,
      })) {
        rowCount += batch.rows;
      }
      return { rowCount, bytesProcessed: fileSize };
    },
    { streaming: true }
  );
}

/**
 * Run all CSV parsers on one file. Returns array of summary objects.
 */
async function runAllCsvParsers(filePath, fileSize, options = {}) {
  const numCols = options.wide ? 100 : 10;
  const results = [];

  const runners = [
    () => runPapaParse(filePath, fileSize),
    () => runCsvParse(filePath, fileSize),
    () => runFastCsv(filePath, fileSize),
    () => runUltratabCsv(filePath, fileSize),
    () => runUltratabColumnar(filePath, fileSize, numCols),
  ];

  for (const run of runners) {
    try {
      const summary = await run();
      results.push(summary);
    } catch (err) {
      results.push({
        name: err.name || "unknown",
        error: err.message,
        streaming: null,
      });
    }
  }
  return results;
}

/**
 * Run CSV benchmarks for a given size and variant. Returns { size, variant, filePath, bytes, rows, results }.
 */
async function runCsvBenchForDataset(sizeName, variant, dataDir) {
  const fileName = `csv_${sizeName}_${variant}.csv`;
  const filePath = path.join(dataDir, fileName);
  if (!fs.existsSync(filePath)) {
    throw new Error(`Dataset not found: ${filePath}. Run npm run bench:generate first.`);
  }
  const stat = fs.statSync(filePath);
  const bytes = stat.size;
  const results = await runAllCsvParsers(filePath, bytes, { wide: variant === "wide" });
  return {
    size: sizeName,
    variant,
    filePath,
    bytes,
    results,
  };
}

module.exports = {
  runPapaParse,
  runCsvParse,
  runFastCsv,
  runUltratabCsv,
  runUltratabColumnar,
  runAllCsvParsers,
  runCsvBenchForDataset,
};
