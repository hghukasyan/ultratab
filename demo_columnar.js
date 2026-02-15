"use strict";

const fs = require("fs");
const path = require("path");
const { csvColumns } = require("./index.js");

const testCsvPath = path.join(__dirname, "demo_columnar_sample.csv");

function createSampleCsv(filePath, rows) {
  const lines = ["id,name,score,active", "1,alice,98.5,true", "2,bob,87.0,false"];
  for (let i = 0; i < rows; i++) {
    lines.push(`${i + 3},user${i},${(Math.random() * 100).toFixed(2)},${i % 2 === 0}`);
  }
  fs.writeFileSync(filePath, lines.join("\n"), "utf8");
}

async function main() {
  console.log("Creating sample CSV (5000 rows with typed columns)...\n");
  createSampleCsv(testCsvPath, 5000);

  console.log("Parsing with csvColumns (schema: int32, float64, bool)...\n");

  let totalRows = 0;
  let batchCount = 0;
  const startTime = process.hrtime.bigint();

  for await (const batch of csvColumns(testCsvPath, {
    headers: true,
    batchSize: 2000,
    schema: {
      id: "int32",
      name: "string",
      score: "float64",
      active: "bool",
    },
    nullValues: ["", "null", "NULL"],
  })) {
    batchCount++;
    totalRows += batch.rows;

    const ids = batch.columns.id;
    const names = batch.columns.name;
    const scores = batch.columns.score;
    const active = batch.columns.active;
    const nullMaskScore = batch.nullMask?.score;

    console.log(
      `  Batch ${batchCount}: ${batch.rows} rows (total: ${totalRows})`
    );
    if (batch.rows > 0) {
      const idx = 0;
      const isNull = nullMaskScore?.[idx];
      console.log(
        `    First row: id=${ids[idx]} (Int32Array), name="${names[idx]}", score=${isNull ? "null" : scores[idx]}, active=${active[idx]}`
      );
    }
  }

  const endTime = process.hrtime.bigint();
  const elapsedMs = Number(endTime - startTime) / 1e6;
  const rowsPerSec = (totalRows / (elapsedMs / 1000)).toFixed(0);
  console.log(
    `\nDone. Total: ${totalRows} rows in ${elapsedMs.toFixed(1)} ms (~${rowsPerSec} rows/sec)`
  );

  fs.unlinkSync(testCsvPath);
  console.log("Cleaned up demo_columnar_sample.csv");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
