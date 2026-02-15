"use strict";

const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");
const config = require("../config");

function ensureDataDir() {
  if (!fs.existsSync(config.DATA_DIR)) {
    fs.mkdirSync(config.DATA_DIR, { recursive: true });
  }
}

/**
 * Target approximate sizes: we generate by row count and accept resulting file size.
 * XLSX compression makes size hard to predict; we aim for ballpark and report actual size.
 */
const XLSX_ROW_TARGETS = {
  small: 25000,   // ~5–15 MB typical
  medium: 500000, // ~50–150 MB
  large: 5000000, // ~500 MB–1 GB
};

/**
 * Generate a single XLSX file with header + data rows. Returns { path, bytes, rows }.
 */
function generateXlsx(filePath, targetRows, options = {}) {
  ensureDataDir();
  const cols = options.cols || 10;
  const header = Array.from({ length: cols }, (_, i) => `Col${i}`);
  const data = [header];
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

/**
 * Generate small/medium (and optional large) XLSX datasets.
 */
function generateAllXlsx() {
  const sizes = config.getActiveSizes();
  const results = {};
  const rowTargets = {
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
