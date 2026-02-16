"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert");
const path = require("path");
const fs = require("fs");
const { xlsx } = require("../index.js");

const fixturePath = path.join(__dirname, "fixture.xlsx");

interface XlsxBatch {
  headers: string[];
  rows: string[][] | Record<string, string[] | Int32Array>;
  rowsCount: number;
}

async function collectBatches(iterable: AsyncIterable<XlsxBatch>): Promise<XlsxBatch[]> {
  const batches: XlsxBatch[] = [];
  for await (const b of iterable) batches.push(b);
  return batches;
}

describe("xlsx streaming", () => {
  it("fixture exists", () => {
    assert.ok(fs.existsSync(fixturePath), "test/fixture.xlsx should exist (run node test/create_fixture_xlsx.js)");
  });

  it("parses fixture with headers", async () => {
    const batches = await collectBatches(xlsx(fixturePath, { headers: true }));
    assert.strictEqual(batches.length, 1, "one batch");
    const b = batches[0];
    assert.deepStrictEqual(b.headers, ["Name", "Value"]);
    assert.strictEqual(b.rowsCount, 2);
    assert.ok(Array.isArray(b.rows));
    const rowsArr = b.rows as string[][];
    assert.strictEqual(rowsArr.length, 2);
    assert.deepStrictEqual(rowsArr[0], ["Hello", "1"]);
    assert.deepStrictEqual(rowsArr[1], ["World", "2"]);
  }, { timeout: 5000 });

  it("sheet by index (1-based)", async () => {
    const batches = await collectBatches(xlsx(fixturePath, { sheet: 1 }));
    assert.strictEqual(batches.length, 1);
    assert.deepStrictEqual(batches[0].headers, ["Name", "Value"]);
    assert.strictEqual(batches[0].rowsCount, 2);
  }, { timeout: 5000 });

  it("schema produces columnar rows", async () => {
    const batches = await collectBatches(
      xlsx(fixturePath, { schema: { Name: "string", Value: "int32" } })
    );
    assert.strictEqual(batches.length, 1);
    const b = batches[0];
    assert.deepStrictEqual(b.headers, ["Name", "Value"]);
    assert.strictEqual(b.rowsCount, 2);
    assert.ok(!Array.isArray(b.rows));
    assert.ok(typeof b.rows === "object" && b.rows !== null);
    const rows = b.rows as Record<string, string[] | Int32Array>;
    assert.ok(Array.isArray(rows.Name));
    assert.ok(rows.Value instanceof Int32Array);
    assert.strictEqual(rows.Name[0], "Hello");
    assert.strictEqual(rows.Name[1], "World");
    assert.strictEqual(rows.Value[0], 1);
    assert.strictEqual(rows.Value[1], 2);
  }, { timeout: 5000 });

  it("select filters columns", async () => {
    const batches = await collectBatches(
      xlsx(fixturePath, { select: ["Value"] })
    );
    assert.strictEqual(batches.length, 1);
    assert.deepStrictEqual(batches[0].headers, ["Value"]);
    assert.strictEqual((batches[0].rows as string[][]).length, 2);
    assert.deepStrictEqual((batches[0].rows as string[][])[0], ["1"]);
    assert.deepStrictEqual((batches[0].rows as string[][])[1], ["2"]);
  }, { timeout: 5000 });

  it("throws on invalid path", async () => {
    await assert.rejects(
      async () => {
        for await (const _ of xlsx("/nonexistent/file.xlsx")) {}
      },
      /failed to create parser|Failed to open|XLSX/
    );
  });
});
