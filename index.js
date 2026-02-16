"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const lib = require("./lib/ultratab.js");
module.exports = {
    csv: lib.csv,
    csvColumns: lib.csvColumns,
    xlsx: lib.xlsx,
    getParserMetrics: lib.getParserMetrics,
    getColumnarParserMetrics: lib.getColumnarParserMetrics,
    createParser: lib.createParser,
    getNextBatch: lib.getNextBatch,
    destroyParser: lib.destroyParser,
    createColumnarParser: lib.createColumnarParser,
    getNextColumnarBatch: lib.getNextColumnarBatch,
    destroyColumnarParser: lib.destroyColumnarParser,
};
