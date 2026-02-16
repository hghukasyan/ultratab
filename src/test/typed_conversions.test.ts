"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { csvColumns } = require("../index.js");

async function withTempCsv(content: string, fn: (p: string) => Promise<void>): Promise<void> {
  const tmp = path.join(os.tmpdir(), `ultratab-test-${Date.now()}.csv`);
  fs.writeFileSync(tmp, content, "utf8");
  try {
    await fn(tmp);
  } finally {
    try {
      fs.unlinkSync(tmp);
    } catch {}
  }
}

interface ColumnarBatch {
  headers: string[];
  columns: Record<string, string[] | Int32Array | BigInt64Array | Float64Array | Uint8Array>;
  nullMask?: Record<string, Uint8Array>;
  rows: number;
}

async function collectBatches(iterable: AsyncIterable<ColumnarBatch>): Promise<ColumnarBatch[]> {
  const batches: ColumnarBatch[] = [];
  for await (const b of iterable) batches.push(b);
  return batches;
}

describe("csvColumns typed conversions", () => {
  it("parses int32", async () => {
    await withTempCsv("x\n0\n1\n-999\n2147483647\n-2147483647\n", async (p) => {
      const batches = await collectBatches(
        csvColumns(p, { schema: { x: "int32" } })
      );
      assert.strictEqual(batches.length, 1);
      const ids = batches[0].columns.x;
      assert.ok(ids instanceof Int32Array);
      assert.strictEqual(ids.length, 5);
      assert.strictEqual(ids[0], 0);
      assert.strictEqual(ids[1], 1);
      assert.strictEqual(ids[2], -999);
      assert.strictEqual(ids[3], 2147483647);
      assert.strictEqual(ids[4], -2147483647);
    });
  });

  it("parses int64/BigInt64Array", async () => {
    await withTempCsv("x\n0\n9223372036854775807\n-9223372036854775807\n", async (p) => {
      const batches = await collectBatches(
        csvColumns(p, { schema: { x: "int64" } })
      );
      assert.strictEqual(batches.length, 1);
      const arr = batches[0].columns.x;
      assert.ok(arr instanceof BigInt64Array);
      assert.strictEqual(arr.length, 3);
      assert.strictEqual(arr[0], 0n);
      assert.strictEqual(arr[1], 9223372036854775807n);
      assert.strictEqual(arr[2], -9223372036854775807n);
    });
  });

  it("parses float64", async () => {
    await withTempCsv("x\n0\n1.5\n-3.14\n1e10\n-0.0025\n", async (p) => {
      const batches = await collectBatches(
        csvColumns(p, { schema: { x: "float64" } })
      );
      assert.strictEqual(batches.length, 1);
      const arr = batches[0].columns.x as Float64Array;
      assert.ok(arr instanceof Float64Array);
      assert.strictEqual(arr.length, 5);
      assert.strictEqual(arr[0], 0);
      assert.strictEqual(arr[1], 1.5);
      assert.strictEqual(arr[2], -3.14);
      assert.strictEqual(arr[3], 1e10);
      assert.ok(Math.abs(arr[4] - (-0.0025)) < 1e-10);
    });
  });

  it("parses bool", async () => {
    await withTempCsv("x\ntrue\nfalse\n1\n0\nTRUE\nFALSE\n", async (p) => {
      const batches = await collectBatches(
        csvColumns(p, { schema: { x: "bool" } })
      );
      assert.strictEqual(batches.length, 1);
      const arr = batches[0].columns.x as Uint8Array;
      assert.ok(arr instanceof Uint8Array);
      assert.strictEqual(arr.length, 6);
      assert.strictEqual(arr[0], 1);
      assert.strictEqual(arr[1], 0);
      assert.strictEqual(arr[2], 1);
      assert.strictEqual(arr[3], 0);
      assert.strictEqual(arr[4], 1);
      assert.strictEqual(arr[5], 0);
    });
  });

  it("nullMask marks nulls for typed columns", async () => {
    await withTempCsv("x\n1\nnull\n3\n\n5\n", async (p) => {
      const batches = await collectBatches(
        csvColumns(p, {
          schema: { x: "int32" },
          nullValues: ["", "null", "NULL"],
        })
      );
      assert.strictEqual(batches.length, 1);
      const nm = batches[0].nullMask?.x;
      assert.ok(nm instanceof Uint8Array, "nullMask.x should exist");
      assert.strictEqual(nm!.length, 5);
      assert.strictEqual(nm![0], 0);
      assert.strictEqual(nm![1], 1);
      assert.strictEqual(nm![2], 0);
      assert.strictEqual(nm![3], 1);
      assert.strictEqual(nm![4], 0);
    });
  });

  it("select filters columns", async () => {
    await withTempCsv("a,b,c\n1,2,3\n4,5,6\n", async (p) => {
      const batches = await collectBatches(
        csvColumns(p, { select: ["a", "c"] })
      );
      assert.strictEqual(batches.length, 1);
      assert.deepStrictEqual(batches[0].headers, ["a", "c"]);
      assert.ok("a" in batches[0].columns);
      assert.ok("c" in batches[0].columns);
      assert.ok(!("b" in batches[0].columns));
    });
  });

  it("trim strips whitespace", async () => {
    await withTempCsv("x\n  1  \n  2\n", async (p) => {
      const batches = await collectBatches(
        csvColumns(p, { schema: { x: "int32" }, trim: true })
      );
      const xCol = batches[0].columns.x as Int32Array;
      assert.strictEqual(xCol.length, 2);
      assert.strictEqual(xCol[0], 1);
      assert.strictEqual(xCol[1], 2);
    });
  });

  it("parse failure uses typedFallback null", async () => {
    await withTempCsv("x\n1\nabc\n3\n", async (p) => {
      const batches = await collectBatches(
        csvColumns(p, { schema: { x: "int32" }, typedFallback: "null" })
      );
      const nm = batches[0].nullMask?.x;
      assert.strictEqual(nm![1], 1);
    });
  });

  it("handles quoted fields with delimiters", async () => {
    await withTempCsv('a,b\n1,"2,3"\n4,5\n', async (p) => {
      const batches = await collectBatches(csvColumns(p));
      const aCol = batches[0].columns.a as string[];
      const bCol = batches[0].columns.b as string[];
      assert.strictEqual(aCol[0], "1");
      assert.strictEqual(bCol[0], "2,3");
    });
  });
});
