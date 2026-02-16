"use strict";

const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");
const config = require("../config");

function ensureDataDir(): void {
  if (!fs.existsSync(config.DATA_DIR)) {
    fs.mkdirSync(config.DATA_DIR, { recursive: true });
  }
}

const XLSX_ROW_TARGETS: Record<string, number> = {
  small: 25000,
  medium: 500000,
  large: 5000000,
};

interface GenerateOptions {
  cols?: number;
}

interface GenerateResult {
  path: string;
  bytes: number;
  rows: number;
}

function generateXlsx(filePath: string, targetRows: number, options: GenerateOptions = {}): GenerateResult {
  ensureDataDir();
  const cols = options.cols || 10;
  const header = Array.from({ length: cols }, (_, i) => `Col${i}`);
  const data: (string | number | boolean)[][] = [header];
  for (let r = 0; r < targetRows; r++) {
    const row = Array.from({ length: cols }, (_, i) => {
      if (i % 4 === 0) return `label_${r}_${i}`;
      if (i % 4 === 1) return r * 1000 + i;
      if (i % 4 === 2) return (r * 3.14 + i).toFixed(2);
      return r % 2 === 0;
    });
    data.push(row);
  }
  const ws = XLSX.utils.aoa_to_sheet(data);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Sheet1");
  XLSX.writeFile(wb, filePath);
  const stat = fs.statSync(filePath);
  return { path: filePath, bytes: stat.size, rows: targetRows };
}

function generateAllXlsx(): Record<string, { path: string; bytes: number; rows: number }> {
  const sizes = config.getActiveSizes();
  const results: Record<string, { path: string; bytes: number; rows: number }> = {};
  const rowTargets: Record<string, number> = {
    small: XLSX_ROW_TARGETS.small,
    medium: XLSX_ROW_TARGETS.medium,
    large: XLSX_ROW_TARGETS.large,
  };
  for (const sizeName of Object.keys(sizes)) {
    const targetRows = rowTargets[sizeName];
    if (!targetRows) continue;
    const name = `xlsx_${sizeName}.xlsx`;
    const filePath = path.join(config.DATA_DIR, name);
    console.log(`Generating ${name} (~${targetRows} rows)...`);
    const result = generateXlsx(filePath, targetRows);
    results[sizeName] = { path: result.path, bytes: result.bytes, rows: result.rows };
    console.log(`  -> ${(result.bytes / 1024 / 1024).toFixed(2)} MB, ${result.rows} rows`);
  }
  return results;
}

if (require.main === module) {
  generateAllXlsx();
}

module.exports = { generateXlsx, generateAllXlsx, ensureDataDir, XLSX_ROW_TARGETS };
