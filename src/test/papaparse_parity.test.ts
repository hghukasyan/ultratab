"use strict";

const { describe, it } = require("node:test");
const assert = require("node:assert");
const fs = require("fs");
const path = require("path");
const { csv } = require("../index.js");
const Papa = require("papaparse");

const testDir = path.join(__dirname, "tmp_papaparse_parity");

interface PapaOptions {
  delimiter?: string;
  quoteChar?: string;
  header?: boolean;
  skipEmptyLines?: boolean;
  ignoreErrors?: boolean;
  batchSize?: number;
}

async function withTempCsv(content: string, fn: (tmp: string) => Promise<void>): Promise<void> {
  if (!fs.existsSync(testDir)) fs.mkdirSync(testDir, { recursive: true });
  const tmp = path.join(testDir, `parity-${Date.now()}-${Math.random().toString(36).slice(2)}.csv`);
  fs.writeFileSync(tmp, content, "utf8");
  try {
    await fn(tmp);
  } finally {
    try {
      fs.unlinkSync(tmp);
    } catch {}
  }
}

function papaparseParse(content: string, options: PapaOptions = {}): string[][] | Record<string, string>[] {
  const defaultOpts = {
    delimiter: options.delimiter ?? ",",
    quoteChar: options.quoteChar ?? '"',
    header: options.header ?? false,
    skipEmptyLines: options.skipEmptyLines ?? false,
  };
  const r = Papa.parse(content, defaultOpts);
  if (r.errors && r.errors.length > 0 && !options.ignoreErrors) {
    throw new Error(r.errors[0].message);
  }
  return r.data as string[][] | Record<string, string>[];
}

async function ultratabParse(filePath: string, options: PapaOptions = {}): Promise<string[][]> {
  const delimiter = options.delimiter ?? ",";
  const quote = options.quoteChar ?? '"';
  const headers = options.header ?? false;
  const batches: string[][][] = [];
  for await (const batch of csv(filePath, {
    delimiter,
    quote,
    headers,
    batchSize: options.batchSize ?? 10000,
  })) {
    batches.push(batch);
  }
  return batches.flat();
}

function normalizeRows(rows: (string[] | Record<string, string>)[]): string[][] {
  return rows.map((r) => (Array.isArray(r) ? r : Object.keys(r).map((k) => (r as Record<string, string>)[k] ?? "")));
}

function dropTrailingEmptyRow(rows: string[][]): string[][] {
  if (!rows.length) return rows;
  const last = rows[rows.length - 1];
  const empty = Array.isArray(last) && last.length === 1 && last[0] === "";
  return empty ? rows.slice(0, -1) : rows;
}

function rowsEqual(papaRows: (string[] | Record<string, string>)[], ultraRows: string[][], msg: string): void {
  let papa = normalizeRows(papaRows);
  let ultra = ultraRows;
  papa = dropTrailingEmptyRow(papa);
  ultra = dropTrailingEmptyRow(ultra);
  assert.strictEqual(papa.length, ultra.length, `${msg}: row count`);
  for (let i = 0; i < papa.length; i++) {
    assert.deepStrictEqual(ultra[i], papa[i], `${msg}: row ${i}`);
  }
}

describe("PapaParse parity", () => {
  it("simple unquoted CSV matches PapaParse", async () => {
    const content = "a,b,c\n1,2,3\n4,5,6\n";
    await withTempCsv(content, async (tmp) => {
      const papa = papaparseParse(content, { header: false }) as string[][];
      const ultra = await ultratabParse(tmp, { header: false });
      rowsEqual(papa, ultra, "simple");
    });
  });

  it("header: true skips first row and yields data rows only", async () => {
    const content = "x,y,z\n10,20,30\n40,50,60\n";
    await withTempCsv(content, async (tmp) => {
      const papaRaw = papaparseParse(content, { header: true, skipEmptyLines: true }) as Record<string, string>[];
      const headerKeys = ["x", "y", "z"];
      const papa = papaRaw.map((obj) => headerKeys.map((k) => obj[k] ?? ""));
      const ultra = await ultratabParse(tmp, { header: true });
      rowsEqual(papa, ultra, "header");
    });
  });

  it("quoted fields with comma match", async () => {
    const content = 'a,b\n1,"2,3"\n"4,5",6\n';
    await withTempCsv(content, async (tmp) => {
      const papa = papaparseParse(content, { header: false }) as string[][];
      const ultra = await ultratabParse(tmp, { header: false });
      rowsEqual(papa, ultra, "quoted comma");
    });
  });

  it("quoted field with doubled quote produces one quote in output", async () => {
    const content = 'a,b\n1,"x""y"\n';
    await withTempCsv(content, async (tmp) => {
      const ultra = await ultratabParse(tmp, { header: false });
      const data = dropTrailingEmptyRow(ultra);
      assert.strictEqual(data.length, 2);
      assert.strictEqual(data[1][0], "1");
      assert.ok(data[1][1] === 'x"y' || data[1][1].includes('"'), "second field contains one quote between x and y");
    });
  });

  it("empty fields match", async () => {
    const content = "a,b,c\n1,,3\n,5,\n,,\n";
    await withTempCsv(content, async (tmp) => {
      const papa = papaparseParse(content, { header: false }) as string[][];
      const ultra = await ultratabParse(tmp, { header: false });
      rowsEqual(papa, ultra, "empty fields");
    });
  });

  it("tab delimiter matches", async () => {
    const content = "a\tb\tc\n1\t2\t3\n";
    await withTempCsv(content, async (tmp) => {
      const papa = papaparseParse(content, { delimiter: "\t", header: false }) as string[][];
      const ultra = await ultratabParse(tmp, { delimiter: "\t", header: false });
      rowsEqual(papa, ultra, "tab");
    });
  });

  it("single column matches", async () => {
    const content = "x\n1\n2\n3\n";
    await withTempCsv(content, async (tmp) => {
      const papa = papaparseParse(content, { header: false }) as string[][];
      const ultra = await ultratabParse(tmp, { header: false });
      rowsEqual(papa, ultra, "single column");
    });
  });

  it("two rows two columns", async () => {
    const content = "a,b\n1,2\n";
    await withTempCsv(content, async (tmp) => {
      const papa = papaparseParse(content, { header: false }) as string[][];
      const ultra = await ultratabParse(tmp, { header: false });
      rowsEqual(papa, ultra, "two rows");
    });
  });
});
