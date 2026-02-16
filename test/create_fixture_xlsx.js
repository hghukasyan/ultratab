"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * Creates test/fixture.xlsx using the xlsx package (SheetJS).
 * Run: node test/create_fixture_xlsx.js
 * Then run: node test/xlsx_streaming.test.js
 */
const XLSX = require("xlsx");
const path = require("path");
const fs = require("fs");
const outPath = path.join(__dirname, "fixture.xlsx");
const data = [
    ["Name", "Value"],
    ["Hello", 1],
    ["World", 2],
];
const ws = XLSX.utils.aoa_to_sheet(data);
const wb = XLSX.utils.book_new();
XLSX.utils.book_append_sheet(wb, ws, "Sheet1");
XLSX.writeFile(wb, outPath);
console.log("Wrote", outPath);
