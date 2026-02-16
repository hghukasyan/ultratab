"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const { describe, it } = require("node:test");
const assert = require("node:assert");
const fs = require("fs");
const path = require("path");
const os = require("os");
// Enable profiling so arena metrics are populated
process.env.ULTRATAB_PROFILE = "1";
const { createParser, getNextBatch, destroyParser, getParserMetrics, createColumnarParser, getNextColumnarBatch, destroyColumnarParser, getColumnarParserMetrics, } = require("../index.js");
const testDir = path.join(__dirname, "..");
const tmpDir = os.tmpdir();
function createTempCsv(rows, cols = 2) {
    const lines = [];
    for (let i = 0; i < rows; i++) {
        const row = Array.from({ length: cols }, (_, j) => `r${i}c${j}`);
        lines.push(row.join(","));
    }
    const file = path.join(tmpDir, `ultratab_arena_${Date.now()}_${Math.random().toString(36).slice(2)}.csv`);
    fs.writeFileSync(file, lines.join("\n") + "\n", "utf8");
    return file;
}
describe("arena memory", () => {
    it("row parser: arena metrics present and arena_resets increases with batches", async () => {
        const file = createTempCsv(25000, 3);
        try {
            const batchSize = 5000;
            const parser = createParser(file, { batchSize });
            assert.ok(parser, "createParser should return a handle");
            let prevResets = 0;
            while ((await getNextBatch(parser)) !== undefined) {
                const m = getParserMetrics(parser);
                assert.ok(typeof m.arena_resets === "number", "arena_resets should be a number");
                assert.ok(m.arena_resets >= prevResets, "arena_resets should be monotonic");
                prevResets = m.arena_resets;
            }
            assert.ok(prevResets >= 1, "arena_resets should reflect batches taken");
            destroyParser(parser);
        }
        finally {
            try {
                fs.unlinkSync(file);
            }
            catch { }
        }
    });
    it("row parser: long streaming parse keeps arena memory bounded", async () => {
        const file = createTempCsv(100000, 4);
        try {
            const batchSize = 10000;
            const parser = createParser(file, { batchSize });
            assert.ok(parser);
            const blockSize = 1024 * 1024; // 1MB
            const maxReasonableBlocks = 32; // arena should not grow unbounded
            let maxBytesSeen = 0;
            while ((await getNextBatch(parser)) !== undefined) {
                const m = getParserMetrics(parser);
                const bytes = m.arena_bytes_allocated ?? 0;
                const blocks = m.arena_blocks ?? 0;
                if (bytes > maxBytesSeen)
                    maxBytesSeen = bytes;
                assert.ok(blocks <= maxReasonableBlocks, `arena_blocks should stay bounded, got ${blocks}`);
            }
            assert.ok(maxBytesSeen <= blockSize * maxReasonableBlocks, "arena_bytes_allocated should be bounded");
            destroyParser(parser);
        }
        finally {
            try {
                fs.unlinkSync(file);
            }
            catch { }
        }
    });
    it("columnar parser: arena metrics and bounded memory over many batches", async () => {
        const file = createTempCsv(150000, 5);
        try {
            const batchSize = 7500;
            const parser = createColumnarParser(file, { batchSize, headers: false });
            assert.ok(parser);
            const blockSize = 1024 * 1024;
            const maxReasonableBlocks = 32;
            let prevResets = 0;
            while ((await getNextColumnarBatch(parser)) !== undefined) {
                const m = getColumnarParserMetrics(parser);
                assert.ok(typeof m.arena_resets === "number");
                assert.ok(m.arena_resets >= prevResets);
                prevResets = m.arena_resets;
                assert.ok((m.arena_blocks ?? 0) <= maxReasonableBlocks);
                assert.ok((m.arena_bytes_allocated ?? 0) <= blockSize * maxReasonableBlocks);
            }
            destroyColumnarParser(parser);
        }
        finally {
            try {
                fs.unlinkSync(file);
            }
            catch { }
        }
    });
    it("stress: many rows parse without leak or crash", async () => {
        const totalRows = 300000;
        const file = createTempCsv(totalRows, 2);
        try {
            const batchSize = 10000;
            const parser = createParser(file, { batchSize });
            let rowsSeen = 0;
            let batch;
            while ((batch = await getNextBatch(parser)) !== undefined) {
                assert.ok(Array.isArray(batch));
                rowsSeen += batch.length;
            }
            assert.ok(rowsSeen >= 200000, `expected at least 200k rows, got ${rowsSeen}`);
            assert.ok(rowsSeen <= totalRows, `expected at most ${totalRows} rows, got ${rowsSeen}`);
            destroyParser(parser);
        }
        finally {
            try {
                fs.unlinkSync(file);
            }
            catch { }
        }
    });
    it("stress: columnar many rows", async () => {
        const dataRows = 150000;
        const header = "a,b,c";
        const lines = [header];
        for (let i = 0; i < dataRows; i++)
            lines.push(`${i},${i * 2},${i % 10}`);
        const file = path.join(tmpDir, `ultratab_arena_col_${Date.now()}.csv`);
        fs.writeFileSync(file, lines.join("\n") + "\n", "utf8");
        try {
            const batchSize = 10000;
            const parser = createColumnarParser(file, { batchSize, headers: true, schema: { a: "int32", b: "int32", c: "int32" } });
            let rowsSeen = 0;
            let result;
            while ((result = await getNextColumnarBatch(parser)) !== undefined) {
                rowsSeen += typeof result.rows === "number" ? result.rows : 0;
            }
            assert.ok(rowsSeen >= 100000, `expected at least 100k columnar rows, got ${rowsSeen}`);
            destroyColumnarParser(parser);
        }
        finally {
            try {
                fs.unlinkSync(file);
            }
            catch { }
        }
    });
});
