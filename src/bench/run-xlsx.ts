"use strict";

const config = require("./config");
const { runXlsxBenchForDataset } = require("./runners/xlsx-runner");
const { printReport } = require("./reporters/console");
const jsonReporter = require("./reporters/json");
const mdReporter = require("./reporters/markdown");

async function main(): Promise<void> {
  const dataDir = config.DATA_DIR;
  const sizes = config.getActiveSizes();
  const sizeFilter = process.env.SIZE;
  if (sizeFilter) {
    if (!(sizeFilter in sizes)) throw new Error(`Unknown SIZE=${sizeFilter}`);
    for (const k of Object.keys(sizes)) {
      if (k !== sizeFilter) delete sizes[k];
    }
  }

  const report: {
    timestamp: string;
    type: string;
    csv: null;
    xlsx: { size: string; bytes: number; results: unknown[] }[];
  } = {
    timestamp: new Date().toISOString(),
    type: "xlsx",
    csv: null,
    xlsx: [],
  };

  for (const sizeName of Object.keys(sizes)) {
    process.stdout.write(`XLSX ${sizeName}... `);
    try {
      const block = await runXlsxBenchForDataset(sizeName, dataDir);
      report.xlsx.push({
        size: block.size,
        bytes: block.bytes,
        results: block.results,
      });
      console.log("OK");
    } catch (err) {
      console.log("FAIL:", (err as Error).message);
    }
  }

  printReport(report);
  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const jsonPath = jsonReporter.writeReport(report, ts);
  const mdPath = mdReporter.writeReport(report, ts);
  console.log("\nReports written:");
  console.log("  ", jsonPath);
  console.log("  ", mdPath);
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
