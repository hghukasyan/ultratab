"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const config = require("../config");
const { generateAllCsv } = require("./generate-csv");
const { generateAllXlsx } = require("./generate-xlsx");
function ensureReportsDir() {
    if (!require("fs").existsSync(config.REPORTS_DIR)) {
        require("fs").mkdirSync(config.REPORTS_DIR, { recursive: true });
    }
}
function main() {
    console.log("Generating benchmark datasets into", config.DATA_DIR);
    console.log("Sizes: small (5MB), medium (100MB)" + (process.env.LARGE === "1" ? ", large (1GB)" : "") + "\n");
    ensureReportsDir();
    const csvManifest = generateAllCsv();
    console.log("");
    const xlsxManifest = generateAllXlsx();
    console.log("\nDone. CSV and XLSX datasets ready.");
}
main();
