"use strict";

/**
 * Creates test/fixture.xlsx using ExcelJS.
 * Run: node test/create_fixture_xlsx.js
 * Then run: node test/xlsx_streaming.test.js
 */
const path = require("path");
const ExcelJS = require("exceljs");

const outPath = path.join(__dirname, "fixture.xlsx");

const data = [
  ["Name", "Value"],
  ["Hello", 1],
  ["World", 2],
];

async function main(): Promise<void> {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("Sheet1");
  ws.addRows(data);
  await wb.xlsx.writeFile(outPath);
  console.log("Wrote", outPath);
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
