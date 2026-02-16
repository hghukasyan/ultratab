"use strict";

interface BenchResult {
  name: string;
  error?: string;
  medianMs?: number;
  p95Ms?: number;
  medianPeakRss?: number;
  p95EventLoopP95?: number;
  rowCount?: number;
  streaming?: boolean | null;
}

function formatResult(r: BenchResult, bytes: number): Record<string, string | number> {
  if (r.error) {
    return { name: r.name, note: `Error: ${r.error}` };
  }
  const mb = bytes / (1024 * 1024);
  const mbPerSec = mb / ((r.medianMs ?? 0) / 1000);
  const rowsPerSec = (r.rowCount ?? 0) / ((r.medianMs ?? 0) / 1000);
  const rssMb = ((r.medianPeakRss ?? 0) / (1024 * 1024));
  const elP95Ms = ((r.p95EventLoopP95 ?? 0) / 1e6);
  return {
    name: r.name,
    "median (ms)": (r.medianMs ?? 0).toFixed(1),
    "p95 (ms)": (r.p95Ms ?? 0).toFixed(1),
    "MB/s": mbPerSec.toFixed(2),
    "rows/s": Math.round(rowsPerSec).toLocaleString(),
    "peak RSS (MB)": rssMb.toFixed(2),
    "event loop p95 (ms)": elP95Ms.toFixed(2),
    streaming: r.streaming === true ? "yes" : r.streaming === false ? "no" : "-",
  };
}

function printCsvBlock(title: string, bytes: number, results: BenchResult[]): void {
  console.log("\n" + "=".repeat(60));
  console.log(title);
  console.log(`Dataset: ${(bytes / 1024 / 1024).toFixed(2)} MB`);
  console.log("=".repeat(60));
  const rows = results.map((r) => formatResult(r, bytes));
  console.table(rows);
}

function printXlsxBlock(title: string, bytes: number, results: BenchResult[]): void {
  console.log("\n" + "=".repeat(60));
  console.log(title);
  console.log(`Dataset: ${(bytes / 1024 / 1024).toFixed(2)} MB`);
  console.log("=".repeat(60));
  const rows = results.map((r) => formatResult(r, bytes));
  console.table(rows);
}

interface Report {
  timestamp: string;
  csv?: { size: string; variant: string; bytes: number; results: BenchResult[] }[];
  xlsx?: { size: string; bytes: number; results: BenchResult[] }[];
}

function printReport(report: Report): void {
  console.log("\nUltratab Benchmark Report");
  console.log("Generated:", report.timestamp);

  if (report.csv) {
    for (const block of report.csv) {
      printCsvBlock(
        `CSV ${block.size} / ${block.variant}`,
        block.bytes,
        block.results
      );
    }
  }

  if (report.xlsx) {
    for (const block of report.xlsx) {
      printXlsxBlock(`XLSX ${block.size}`, block.bytes, block.results);
    }
  }
}

module.exports = { formatResult, printCsvBlock, printXlsxBlock, printReport };
