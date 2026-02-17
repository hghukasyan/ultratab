"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const fs = require("fs");
const path = require("path");
const { xlsx: ultratabXlsx } = require("../../index.js");
const { runBenchmark } = require("../lib/run-bench");
const config = require("../config");
const BATCH_SIZE = config.DEFAULT_BATCH_SIZE;
async function runExcelJS(filePath, fileSize) {
    let ExcelJS;
    try {
        ExcelJS = require("exceljs");
    }
    catch {
        return { name: "exceljs (stream)", error: "exceljs not installed", streaming: null };
    }
    return runBenchmark("exceljs (stream)", async () => {
        let rowCount = 0;
        const stream = fs.createReadStream(filePath);
        const reader = new ExcelJS.stream.xlsx.WorkbookReader(stream, {});
        for await (const worksheetReader of reader) {
            for await (const _row of worksheetReader) {
                rowCount++;
            }
        }
        return { rowCount, bytesProcessed: fileSize };
    }, { streaming: true }).catch((err) => ({
        name: "exceljs (stream)",
        error: err.message,
        streaming: true,
    }));
}
async function runUltratabXlsx(filePath, fileSize) {
    return runBenchmark("ultratab xlsx", async () => {
        let rowCount = 0;
        for await (const batch of ultratabXlsx(filePath, {
            headers: true,
            batchSize: BATCH_SIZE,
        })) {
            rowCount += batch.rowsCount;
        }
        return { rowCount, bytesProcessed: fileSize };
    }, { streaming: true });
}
async function runAllXlsxParsers(filePath, fileSize) {
    const results = [];
    const runners = [
        () => runExcelJS(filePath, fileSize),
        () => runUltratabXlsx(filePath, fileSize),
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
async function runXlsxBenchForDataset(sizeName, dataDir) {
    const fileName = `xlsx_${sizeName}.xlsx`;
    const filePath = path.join(dataDir, fileName);
    if (!fs.existsSync(filePath)) {
        throw new Error(`Dataset not found: ${filePath}. Run npm run bench:generate first.`);
    }
    const stat = fs.statSync(filePath);
    const bytes = stat.size;
    const results = await runAllXlsxParsers(filePath, bytes);
    return {
        size: sizeName,
        filePath,
        bytes,
        results,
    };
}
module.exports = {
    runExcelJS,
    runUltratabXlsx,
    runAllXlsxParsers,
    runXlsxBenchForDataset,
};
