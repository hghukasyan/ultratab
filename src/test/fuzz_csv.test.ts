"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert");
const fs = require("fs");
const path = require("path");
const { csv } = require("../index.js");
const Papa = require("papaparse");

const testDir = path.join(__dirname, "tmp_fuzz");

interface FuzzOptions {
  delimiter?: string;
  quoteChar?: string;
  header?: boolean;
  maxCellLen?: number;
}

async function withTempCsv(content: string, fn: (tmp: string) => Promise<void>): Promise<void> {
  if (!fs.existsSync(testDir)) fs.mkdirSync(testDir, { recursive: true });
  const tmp = path.join(testDir, `fuzz-${Date.now()}-${Math.random().toString(36).slice(2)}.csv`);
  fs.writeFileSync(tmp, content, "utf8");
  try {
    await fn(tmp);
  } finally {
    try {
      fs.unlinkSync(tmp);
    } catch {}
  }
}

function papaparseParse(content: string, options: FuzzOptions = {}): string[][] {
  const r = Papa.parse(content, {
    delimiter: options.delimiter ?? ",",
    quoteChar: options.quoteChar ?? '"',
    header: options.header ?? false,
    skipEmptyLines: false,
  });
  return r.data as string[][];
}

async function ultratabParse(filePath: string, options: FuzzOptions = {}): Promise<string[][]> {
  const batches: string[][][] = [];
  for await (const batch of csv(filePath, {
    delimiter: options.delimiter ?? ",",
    quote: options.quoteChar ?? '"',
    headers: options.header ?? false,
    batchSize: 5000,
  })) {
    batches.push(batch);
  }
  return batches.flat();
}

function rowsEqual(a: string[][], b: string[][]): void {
  assert.strictEqual(a.length, b.length, "row count");
  for (let i = 0; i < a.length; i++) {
    assert.deepStrictEqual(a[i], b[i], `row ${i}`);
  }
}

function randomInt(lo: number, hi: number): number {
  return lo + Math.floor(Math.random() * (hi - lo + 1));
}

function generateSimpleRandomCsv(rows: number, cols: number, options: FuzzOptions = {}): string {
  const delimiter = options.delimiter ?? ",";
  const lines: string[] = [];
  for (let r = 0; r < rows; r++) {
    const cells: string[] = [];
    for (let c = 0; c < cols; c++) {
      let cell = "";
      const len = randomInt(0, options.maxCellLen ?? 12);
      for (let i = 0; i < len; i++) {
        const roll = Math.random();
        if (roll < 0.8) {
          cell += String.fromCharCode(randomInt(97, 122));
        } else {
          cell += String.fromCharCode(randomInt(48, 57));
        }
      }
      cells.push(cell);
    }
    lines.push(cells.join(delimiter));
  }
  return lines.join("\n");
}

function dropTrailingEmpty(rows: string[][]): string[][] {
  if (!rows.length) return rows;
  const last = rows[rows.length - 1];
  if (Array.isArray(last) && last.length === 1 && last[0] === "") return rows.slice(0, -1);
  return rows;
}

describe("Fuzz: random CSV vs PapaParse (simple content)", () => {
  for (let seed = 0; seed < 5; seed++) {
    it(`random small CSV seed ${seed} matches PapaParse`, async () => {
      const content = generateSimpleRandomCsv(20, 5, { maxCellLen: 8 }) + "\n";
      await withTempCsv(content, async (tmp) => {
        const papa = dropTrailingEmpty(papaparseParse(content, { header: false }));
        const ultra = dropTrailingEmpty(await ultratabParse(tmp, { header: false }));
        rowsEqual(papa, ultra);
      });
    });
  }

  it("larger random CSV (100 rows, 10 cols) matches", async () => {
    const content = generateSimpleRandomCsv(100, 10, { maxCellLen: 12 }) + "\n";
    await withTempCsv(content, async (tmp) => {
      const papa = dropTrailingEmpty(papaparseParse(content, { header: false }));
      const ultra = dropTrailingEmpty(await ultratabParse(tmp, { header: false }));
      rowsEqual(papa, ultra);
    });
  });

  it("numeric-heavy random CSV matches", async () => {
    const rows = 50;
    const cols = 6;
    const lines: string[] = [];
    for (let r = 0; r < rows; r++) {
      const cells: string[] = [];
      for (let c = 0; c < cols; c++) {
        cells.push(String(r * 1000 + c * 3.14));
      }
      lines.push(cells.join(","));
    }
    const content = lines.join("\n") + "\n";
    await withTempCsv(content, async (tmp) => {
      const papa = dropTrailingEmpty(papaparseParse(content, { header: false }));
      const ultra = dropTrailingEmpty(await ultratabParse(tmp, { header: false }));
      rowsEqual(papa, ultra);
    });
  });
});
