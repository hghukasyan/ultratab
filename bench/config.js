"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
const BENCH_ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(BENCH_ROOT, "bench", "data");
const REPORTS_DIR = path.join(BENCH_ROOT, "bench", "reports");
const SIZES = {
    small: 5 * 1024 * 1024, // 5 MB
    medium: 100 * 1024 * 1024, // 100 MB
    large: 1024 * 1024 * 1024, // 1 GB
};
const CSV_VARIANTS = [
    "simple",
    "quoted",
    "multiline",
    "wide",
    "numeric_heavy",
    "string_heavy",
    "missing",
];
const DEFAULT_BATCH_SIZE = 10000;
const WARMUP_RUNS = 2;
const ITERATIONS = 5;
const EVENT_LOOP_RESOLUTION_MS = 10;
function getActiveSizes() {
    const sizes = { small: SIZES.small, medium: SIZES.medium };
    if (process.env.LARGE === "1")
        sizes.large = SIZES.large;
    return sizes;
}
module.exports = {
    BENCH_ROOT,
    DATA_DIR,
    REPORTS_DIR,
    SIZES,
    CSV_VARIANTS,
    DEFAULT_BATCH_SIZE,
    WARMUP_RUNS,
    ITERATIONS,
    EVENT_LOOP_RESOLUTION_MS,
    getActiveSizes,
};
