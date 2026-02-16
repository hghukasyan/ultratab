"use strict";

const fs = require("fs");
const path = require("path");
const config = require("../config");

function ensureDataDir(): void {
  if (!fs.existsSync(config.DATA_DIR)) {
    fs.mkdirSync(config.DATA_DIR, { recursive: true });
  }
}

interface GenerateResult {
  path: string;
  bytes: number;
  rows: number;
}

function generateCsv(filePath: string, targetBytes: number, variant: string): GenerateResult {
  ensureDataDir();
  const fd = fs.openSync(filePath, "w");
  const bufSize = 256 * 1024;
  let buf = "";
  let bytes = 0;
  let rows = 0;

  const flush = (): void => {
    if (buf.length > 0) {
      fs.writeSync(fd, buf);
      bytes += Buffer.byteLength(buf, "utf8");
      buf = "";
    }
  };

  const cols = variant === "wide" ? 100 : 10;
  const header = Array.from({ length: cols }, (_, i) => `col${i}`).join(",") + "\n";
  fs.writeSync(fd, header);
  bytes += Buffer.byteLength(header, "utf8");
  rows++;

  const generators: Record<string, () => string> = {
    simple() {
      return Array.from({ length: cols }, (_, i) => `${rows}_${i}`).join(",") + "\n";
    },
    quoted() {
      return Array.from({ length: cols }, (_, i) => `"field ${rows}_${i} with, comma"`).join(",") + "\n";
    },
    multiline() {
      return Array.from({ length: cols }, (_, i) => `"line1\nline2\nrow${rows}"`).join(",") + "\n";
    },
    wide() {
      return Array.from({ length: cols }, (_, i) => `r${rows}_${i}`).join(",") + "\n";
    },
    numeric_heavy() {
      const parts: string[] = [];
      for (let i = 0; i < cols; i++) {
        parts.push(i % 3 === 0 ? `label_${i}` : String(rows * 1000 + i * 3.14));
      }
      return parts.join(",") + "\n";
    },
    string_heavy() {
      return Array.from({ length: cols }, (_, i) => `text_${rows}_${i}_extra_long_value`).join(",") + "\n";
    },
    missing() {
      return Array.from({ length: cols }, (_, i) => (i % 4 === 0 ? "" : `v${rows}_${i}`)).join(",") + "\n";
    },
  };

  const gen = generators[variant] || generators.simple;

  while (bytes < targetBytes) {
    buf += gen();
    rows++;
    if (buf.length >= bufSize) flush();
  }
  flush();
  fs.closeSync(fd);
  return { path: filePath, bytes, rows };
}

function generateAllCsv(): Record<string, Record<string, { path: string; bytes: number; rows: number }>> {
  const sizes = config.getActiveSizes();
  const results: Record<string, Record<string, { path: string; bytes: number; rows: number }>> = {};
  for (const [sizeName, targetBytes] of Object.entries(sizes) as [string, number][]) {
    results[sizeName] = {};
    for (const variant of config.CSV_VARIANTS) {
      const name = `csv_${sizeName}_${variant}.csv`;
      const filePath = path.join(config.DATA_DIR, name);
      console.log(`Generating ${name} (target ${(targetBytes / 1024 / 1024).toFixed(1)} MB)...`);
      const result = generateCsv(filePath, targetBytes as number, variant);
      results[sizeName][variant] = { path: result.path, bytes: result.bytes, rows: result.rows };
      console.log(`  -> ${(result.bytes / 1024 / 1024).toFixed(2)} MB, ${result.rows} rows`);
    }
  }
  return results;
}

if (require.main === module) {
  generateAllCsv();
}

module.exports = { generateCsv, generateAllCsv, ensureDataDir };
