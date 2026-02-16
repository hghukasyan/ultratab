"use strict";

const config = require("./config");
const { runCsvBenchForDataset } = require("./runners/csv-runner");
const { runXlsxBenchForDataset } = require("./runners/xlsx-runner");
const { printReport } = require("./reporters/console");
const jsonReporter = require("./reporters/json");
const mdReporter = require("./reporters/markdown");

async function main(): Promise<void> {
  const dataDir = config.DATA_DIR;
  const sizes = config.getActiveSizes();
  let csvVariants = config.CSV_VARIANTS;
  const sizeFilter = process.env.SIZE;
  const variantFilter = process.env.VARIANT;
  if (sizeFilter) {
    if (!(sizeFilter in sizes)) throw new Error(`Unknown SIZE=${sizeFilter}`);
    for (const k of Object.keys(sizes)) {
      if (k !== sizeFilter) delete sizes[k];
    }
  }
  if (variantFilter) {
    if (!csvVariants.includes(variantFilter)) throw new Error(`Unknown VARIANT=${variantFilter}`);
    csvVariants = [variantFilter];
  }

  const report: {
    timestamp: string;
    type: string;
    csv: { size: string; variant: string; bytes: number; results: unknown[] }[];
    xlsx: { size: string; bytes: number; results: unknown[] }[];
  } = {
    timestamp: new Date().toISOString(),
    type: "all",
    csv: [],
    xlsx: [],
  };

  for (const sizeName of Object.keys(sizes)) {
    for (const variant of csvVariants) {
      process.stdout.write(`CSV ${sizeName} / ${variant}... `);
      try {
        const block = await runCsvBenchForDataset(sizeName, variant, dataDir);
        report.csv.push({
          size: block.size,
          variant: block.variant,
          bytes: block.bytes,
          results: block.results,
        });
        console.log("OK");
      } catch (err) {
        console.log("FAIL:", (err as Error).message);
      }
    }

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
