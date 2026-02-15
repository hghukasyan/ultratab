"use strict";

const path = require("path");
const { xlsx } = require("./index.js");

const fixturePath = path.join(__dirname, "test", "fixture.xlsx");

async function main() {
  console.log("Streaming XLSX (test/fixture.xlsx):\n");

  for await (const batch of xlsx(fixturePath, { batchSize: 1000 })) {
    console.log("Headers:", batch.headers);
    console.log("Rows in batch:", batch.rowsCount);
    if (Array.isArray(batch.rows)) {
      for (let i = 0; i < Math.min(5, batch.rows.length); i++) {
        console.log("  Row", i + 1, batch.rows[i]);
      }
      if (batch.rows.length > 5) console.log("  ...");
    } else {
      console.log("  (columnar)", Object.keys(batch.rows));
    }
    console.log();
  }

  console.log("With schema (typed columns):\n");
  for await (const batch of xlsx(fixturePath, {
    schema: { Name: "string", Value: "int32" },
  })) {
    console.log("Headers:", batch.headers);
    console.log("Rows:", batch.rowsCount);
    const names = batch.rows.Name;
    const values = batch.rows.Value;
    for (let i = 0; i < batch.rowsCount; i++) {
      console.log("  ", names[i], "->", values[i]);
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
