"use strict";

const fs = require("fs");
const path = require("path");
const config = require("../config");

function formatResultRow(r, bytes) {
  if (r.error) {
    return `| ${r.name} | - | - | - | - | - | - | ${r.error} |`;
  }
  const mb = bytes / (1024 * 1024);
  const mbPerSec = (mb / (r.medianMs / 1000)).toFixed(2);
  const rowsPerSec = Math.round(r.rowCount / (r.medianMs / 1000)).toLocaleString();
  const rssMb = ((r.medianPeakRss || 0) / (1024 * 1024)).toFixed(2);
  const elP95Ms = ((r.p95EventLoopP95 || 0) / 1e6).toFixed(2);
  const stream = r.streaming === true ? "yes" : r.streaming === false ? "no" : "-";
  return `| ${r.name} | ${r.medianMs.toFixed(1)} | ${r.p95Ms.toFixed(1)} | ${mbPerSec} | ${rowsPerSec} | ${rssMb} | ${elP95Ms} | ${stream} |`;
}

function sectionCsv(size, variant, bytes, results) {
  const header = "| Parser | Median (ms) | P95 (ms) | MB/s | rows/s | Peak RSS (MB) | Event loop p95 (ms) | Streaming |";
  const sep = "| --- | --- | --- | --- | --- | --- | --- | --- |";
  const rows = results.map((r) => formatResultRow(r, bytes)).join("\n");
  return `### CSV – ${size} / ${variant}\n\nDataset: ${(bytes / 1024 / 1024).toFixed(2)} MB\n\n${header}\n${sep}\n${rows}\n`;
}

function sectionXlsx(size, bytes, results) {
  const header = "| Parser | Median (ms) | P95 (ms) | MB/s | rows/s | Peak RSS (MB) | Event loop p95 (ms) | Streaming |";
  const sep = "| --- | --- | --- | --- | --- | --- | --- | --- |";
  const rows = results.map((r) => formatResultRow(r, bytes)).join("\n");
  return `### XLSX – ${size}\n\nDataset: ${(bytes / 1024 / 1024).toFixed(2)} MB\n\n${header}\n${sep}\n${rows}\n`;
}

function toMarkdown(report) {
  const lines = [
    "# Ultratab Benchmark Report",
    "",
    `**Generated:** ${report.timestamp}`,
    "",
    "---",
    "",
  ];
  if (report.csv && report.csv.length) {
    lines.push("## CSV");
    lines.push("");
    for (const block of report.csv) {
      lines.push(sectionCsv(block.size, block.variant, block.bytes, block.results));
    }
  }
  if (report.xlsx && report.xlsx.length) {
    lines.push("## XLSX");
    lines.push("");
    for (const block of report.xlsx) {
      lines.push(sectionXlsx(block.size, block.bytes, block.results));
    }
  }
  return lines.join("\n");
}

function writeReport(report, timestamp) {
  if (!timestamp) timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  if (!fs.existsSync(config.REPORTS_DIR)) {
    fs.mkdirSync(config.REPORTS_DIR, { recursive: true });
  }
  const filePath = path.join(config.REPORTS_DIR, `${timestamp}.md`);
  fs.writeFileSync(filePath, toMarkdown(report), "utf8");
  return filePath;
}

module.exports = { toMarkdown, writeReport, formatResultRow, sectionCsv, sectionXlsx };
