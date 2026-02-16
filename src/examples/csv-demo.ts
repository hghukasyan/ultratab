"use strict";

/**
 * Minimal example: stream a CSV file with ultratab.
 *
 * Run from project root: node examples/csv-demo.js
 * Or after npm install ultratab: node node_modules/ultratab/examples/csv-demo.js
 *
 * Uses examples/data.csv (included). For your own file:
 *   import { csv } from "ultratab";
 *   for await (const batch of csv("path/to/file.csv")) {
 *     console.log(batch.length);
 *   }
 */

const path = require("path");
const fs = require("fs");
const { csv } = require("../index.js");

const dataPath = path.join(__dirname, "data.csv");

async function main(): Promise<void> {
  if (!fs.existsSync(dataPath)) {
    console.error("examples/data.csv not found. Create it or use your own CSV path.");
    process.exit(1);
  }

  let totalRows = 0;
  for await (const batch of csv(dataPath, { batchSize: 10000, headers: true })) {
    totalRows += batch.length;
    console.log("Batch:", batch.length, "rows");
  }
  console.log("Total rows:", totalRows);
}

main().catch((err: unknown) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
