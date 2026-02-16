"use strict";

const fs = require("fs");
const path = require("path");
const { csv } = require("./index.js");

const testCsvPath = path.join(__dirname, "demo_sample.csv");

function createSampleCsv(filePath: string, rows: number): void {
  const lines = ["a,b,c", "1,2,3", '4,"five\nwith newline",6', '7,"8""nine",10'];
  for (let i = 0; i < rows; i++) {
    lines.push(`${i},${i * 2},${i * 3}`);
  }
  fs.writeFileSync(filePath, lines.join("\n"), "utf8");
}

async function main(): Promise<void> {
  const minimalPath = path.join(__dirname, "demo_minimal.csv");
  fs.writeFileSync(minimalPath, "a,b\n1,2\n", "utf8");
  let count = 0;
  for await (const batch of csv(minimalPath, { headers: true })) {
    count += batch.length;
    if (batch.length > 0 && batch[0].length !== 2) {
      console.error("Expected first row to have 2 fields, got:", batch[0].length);
      process.exit(1);
    }
  }
  if (count !== 1) {
    console.error("Expected 1 data row, got:", count);
    process.exit(1);
  }
  fs.unlinkSync(minimalPath);
  console.log("Minimal test OK (1 row, 2 fields).\n");

  console.log("Creating sample CSV (1000 rows)...");
  createSampleCsv(testCsvPath, 1000);

  console.log("Parsing with UltraTab (headers: true, batchSize: 500)...\n");

  let totalRows = 0;
  let batchCount = 0;

  for await (const batch of csv(testCsvPath, { headers: true, batchSize: 500 })) {
    batchCount++;
    totalRows += batch.length;
    const first = batch.length > 0 ? batch[0] : [];
    const preview =
      first.length > 5
        ? JSON.stringify(first.slice(0, 5)) + " ..."
        : JSON.stringify(first);
    console.log(
      `  Batch ${batchCount}: ${batch.length} rows (total: ${totalRows}), first row preview: ${preview}`
    );
  }

  console.log(`\nDone. Total batches: ${batchCount}, total rows: ${totalRows}`);

  const expectedRows = 4 + 1000 - 1;
  if (totalRows !== expectedRows) {
    console.error(`Expected ${expectedRows} rows, got ${totalRows}`);
    process.exit(1);
  }

  fs.unlinkSync(testCsvPath);
  console.log("Cleaned up demo_sample.csv");
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
