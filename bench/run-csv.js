"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const config = require("./config");
const { runCsvBenchForDataset } = require("./runners/csv-runner");
const { printReport } = require("./reporters/console");
const jsonReporter = require("./reporters/json");
const mdReporter = require("./reporters/markdown");
async function main() {
    const dataDir = config.DATA_DIR;
    const sizes = config.getActiveSizes();
    let variants = config.CSV_VARIANTS;
    const sizeFilter = process.env.SIZE;
    const variantFilter = process.env.VARIANT;
    if (sizeFilter) {
        if (!(sizeFilter in sizes))
            throw new Error(`Unknown SIZE=${sizeFilter}`);
        for (const k of Object.keys(sizes)) {
            if (k !== sizeFilter)
                delete sizes[k];
        }
    }
    if (variantFilter) {
        if (!variants.includes(variantFilter))
            throw new Error(`Unknown VARIANT=${variantFilter}`);
        variants = [variantFilter];
    }
    const report = {
        timestamp: new Date().toISOString(),
        type: "csv",
        csv: [],
        xlsx: null,
    };
    for (const sizeName of Object.keys(sizes)) {
        for (const variant of variants) {
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
            }
            catch (err) {
                console.log("FAIL:", err.message);
            }
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
main().catch((err) => {
    console.error(err);
    process.exit(1);
});
