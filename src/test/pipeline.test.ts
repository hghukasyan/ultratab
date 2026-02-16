"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert");
const fs = require("fs");
const path = require("path");
const { csv, csvColumns } = require("../index.js");

const testDir = path.join(__dirname, "..");
const fixtureCsv = path.join(testDir, "test", "pipeline_fixture.csv");

function ensureFixture(): string {
  const rows = ["a,b"];
  for (let i = 0; i < 50000; i++) rows.push(`${i},${i * 2}`);
  fs.writeFileSync(fixtureCsv, rows.join("\n") + "\n", "utf8");
  return fixtureCsv;
}

async function collectBatches<T>(iterable: AsyncIterable<T>, maxBatches = Infinity): Promise<T[]> {
  const batches: T[] = [];
  for await (const b of iterable) {
    batches.push(b);
    if (batches.length >= maxBatches) break;
  }
  return batches;
}

describe("pipeline", () => {
  it("backpressure: queue bounded with maxQueueBatches=1", async () => {
    const p = ensureFixture();
    const batches = await collectBatches(
      csv(p, { batchSize: 10000, maxQueueBatches: 1 })
    ) as string[][][];
    assert.ok(batches.length >= 1);
    assert.ok(batches.length <= 6);
    const totalRows = batches.reduce((s: number, b: string[][]) => s + b.length, 0);
    assert.ok(totalRows >= 50000 && totalRows <= 50001, `expected 50000-50001 rows, got ${totalRows}`);
  });

  it("early cancel: return() stops iteration and releases", async () => {
    const p = ensureFixture();
    const it = csv(p, { batchSize: 5000 })[Symbol.asyncIterator]();
    const { value: first } = await it.next();
    assert.ok(Array.isArray(first));
    assert.ok(first!.length > 0);
    const result = await it.return?.();
    assert.strictEqual(result?.done, true);
    assert.strictEqual(result?.value, undefined);
  });

  it("early cancel columnar: return() stops", async () => {
    const p = ensureFixture();
    const it = csvColumns(p, { batchSize: 5000, schema: { a: "int32", b: "int32" } })[
      Symbol.asyncIterator
    ]();
    const { value: first } = await it.next();
    assert.ok(first && typeof (first as { rows: number }).rows === "number");
    const result = await it.return?.();
    assert.strictEqual(result?.done, true);
  });

  it("metrics exposed when getParserMetrics called", async () => {
    const {
      createParser,
      getNextBatch,
      destroyParser,
      getParserMetrics: getMetrics,
    } = require("../index.js");
    const p = ensureFixture();
    const parser = createParser(p, { batchSize: 1000 });
    if (!parser) return;
    const first = await getNextBatch(parser);
    assert.ok(Array.isArray(first));
    const metrics = getMetrics(parser);
    assert.ok(metrics && typeof metrics.bytes_read === "number");
    assert.ok(typeof metrics.rows_parsed === "number");
    assert.ok(typeof metrics.batches_emitted === "number");
    while ((await getNextBatch(parser)) !== undefined) {}
    destroyParser(parser);
  });
});
