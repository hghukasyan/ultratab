"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const fs = require("fs");
const path = require("path");
const { csv, csvColumns } = require("../../index.js");
const Papa = require("papaparse");
const { parse } = require("csv-parse");
const { parseStream } = require("@fast-csv/parse");
const { runBenchmark } = require("../lib/run-bench");
const config = require("../config");
const BATCH_SIZE = config.DEFAULT_BATCH_SIZE;
async function runPapaParse(filePath, fileSize) {
    return runBenchmark("papaparse (stream)", async () => {
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
    }, { streaming: true });
}
async function runCsvParse(filePath, fileSize) {
    return runBenchmark("csv-parse (stream)", async () => {
        return new Promise((resolve, reject) => {
            let rowCount = 0;
            const parser = parse({ delimiter: ",", relax_quotes: true });
            let first = true;
            fs.createReadStream(filePath)
                .pipe(parser)
                .on("data", () => {
                if (first) {
                    first = false;
                    return;
                }
                rowCount++;
            })
                .on("end", () => resolve({ rowCount, bytesProcessed: fileSize }))
                .on("error", reject);
        });
    }, { streaming: true });
}
async function runFastCsv(filePath, fileSize) {
    return runBenchmark("fast-csv (stream)", async () => {
        return new Promise((resolve, reject) => {
            let rowCount = 0;
            const stream = fs.createReadStream(filePath);
            let first = true;
            parseStream(stream, { headers: false })
                .on("data", () => {
                if (first) {
                    first = false;
                    return;
                }
                rowCount++;
            })
                .on("end", () => resolve({ rowCount, bytesProcessed: fileSize }))
                .on("error", reject);
        });
    }, { streaming: true });
}
async function runUltratabCsv(filePath, fileSize) {
    return runBenchmark("ultratab (string batches)", async () => {
        let rowCount = 0;
        for await (const batch of csv(filePath, { headers: false, batchSize: BATCH_SIZE })) {
            rowCount += batch.length;
        }
        return { rowCount, bytesProcessed: fileSize };
    }, { streaming: true });
}
async function runUltratabColumnar(filePath, fileSize, numCols = 10) {
    const schema = {};
    for (let i = 0; i < numCols; i++) {
        schema[`col${i}`] = i % 3 === 0 ? "string" : "float64";
    }
    return runBenchmark("ultratab (columnar typed)", async () => {
        let rowCount = 0;
        for await (const batch of csvColumns(filePath, {
            headers: true,
            batchSize: BATCH_SIZE,
            schema,
        })) {
            rowCount += batch.rows;
        }
        return { rowCount, bytesProcessed: fileSize };
    }, { streaming: true });
}
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
        }
        catch (err) {
            results.push({
                name: err.name || "unknown",
                error: err.message,
                streaming: null,
            });
        }
    }
    return results;
}
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
