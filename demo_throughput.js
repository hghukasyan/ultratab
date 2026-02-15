"use strict";

const fs = require("fs");
const path = require("path");
const { csv, csvColumns } = require("./index.js");

function createLargeCsv(filePath, rows) {
  const fd = fs.openSync(filePath, "w");
  const header = "id,name,value,flag\n";
  fs.writeSync(fd, header);
  const bufSize = 64 * 1024;
  let buf = "";
  for (let i = 0; i < rows; i++) {
    buf += `${i},user${i},${(i * 3.14).toFixed(2)},${i % 2 === 0}\n`;
    if (buf.length >= bufSize) {
      fs.writeSync(fd, buf);
      buf = "";
    }
  }
  if (buf.length > 0) fs.writeSync(fd, buf);
  fs.closeSync(fd);
}

async function measureCsv(filePath, batchSize) {
  let totalRows = 0;
  const start = process.hrtime.bigint();
  for await (const batch of csv(filePath, { headers: true, batchSize })) {
    totalRows += batch.length;
  }
  const end = process.hrtime.bigint();
  return { rows: totalRows, ms: Number(end - start) / 1e6 };
}

async function measureCsvColumns(filePath, batchSize) {
  let totalRows = 0;
  const start = process.hrtime.bigint();
  for await (const batch of csvColumns(filePath, {
    headers: true,
    batchSize,
    schema: { id: "int32", name: "string", value: "float64", flag: "bool" },
  })) {
    totalRows += batch.rows;
  }
  const end = process.hrtime.bigint();
  return { rows: totalRows, ms: Number(end - start) / 1e6 };
}

async function main() {
  const rows = process.env.ROWS ? parseInt(process.env.ROWS, 10) : 500000;
  const csvPath = path.join(__dirname, "demo_throughput_sample.csv");

  console.log(`Creating CSV with ${rows.toLocaleString()} rows...`);
  const createStart = process.hrtime.bigint();
  createLargeCsv(csvPath, rows);
  const createEnd = process.hrtime.bigint();
  const fileSize = fs.statSync(csvPath).size;
  console.log(
    `  Created ${(fileSize / 1024 / 1024).toFixed(2)} MB in ${Number(createEnd - createStart) / 1e6} ms\n`
  );

  console.log("Benchmark: csv() row-based API");
  const csvResult = await measureCsv(csvPath, 10000);
  const csvRps = (csvResult.rows / (csvResult.ms / 1000)).toFixed(0);
  console.log(
    `  ${csvResult.rows.toLocaleString()} rows in ${csvResult.ms.toFixed(1)} ms = ${csvRps} rows/sec\n`
  );

  console.log("Benchmark: csvColumns() typed columnar API");
  const colResult = await measureCsvColumns(csvPath, 10000);
  const colRps = (colResult.rows / (colResult.ms / 1000)).toFixed(0);
  console.log(
    `  ${colResult.rows.toLocaleString()} rows in ${colResult.ms.toFixed(1)} ms = ${colRps} rows/sec\n`
  );

  fs.unlinkSync(csvPath);
  console.log("Cleaned up. Use ROWS=1000000 node demo_throughput.js for 1M rows.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
