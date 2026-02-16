"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const fs = require("fs");
const path = require("path");
const config = require("../config");
function toJSON(report) {
    return JSON.stringify(report, null, 2);
}
function writeReport(report, timestamp) {
    if (!timestamp)
        timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
    if (!fs.existsSync(config.REPORTS_DIR)) {
        fs.mkdirSync(config.REPORTS_DIR, { recursive: true });
    }
    const filePath = path.join(config.REPORTS_DIR, `${timestamp}.json`);
    fs.writeFileSync(filePath, toJSON(report), "utf8");
    return filePath;
}
module.exports = { toJSON, writeReport };
