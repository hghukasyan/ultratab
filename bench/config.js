"use strict";

const path = require("path");

const BENCH_ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(BENCH_ROOT, "bench", "data");
const REPORTS_DIR = path.join(BENCH_ROOT, "bench", "reports");

/** Target sizes in bytes. large is optional (set LARGE=1 to enable). */
const SIZES = {
  small: 5 * 1024 * 1024,   // 5 MB
  medium: 100 * 1024 * 1024, // 100 MB
  large: 1024 * 1024 * 1024, // 1 GB
};

/** CSV dataset variants for fairness. */
const CSV_VARIANTS = [
  "simple",        // no quotes, minimal escaping
  "quoted",        // many quoted fields
  "multiline",     // quoted fields with newlines
  "wide",          // 100 columns
  "numeric_heavy", // mostly numbers
  "string_heavy",  // mostly strings
  "missing",       // empty/missing values
];

/** Default batch size for streaming parsers (rows). */
const DEFAULT_BATCH_SIZE = 10000;

/** Warmup runs before timed iterations. */
const WARMUP_RUNS = 2;

/** Timed iterations; median and p95 computed from these. */
const ITERATIONS = 5;

/** Event loop monitor resolution (ms). */
const EVENT_LOOP_RESOLUTION_MS = 10;

/** Enable 1GB large dataset only when LARGE=1. */
function getActiveSizes() {
  const sizes = { small: SIZES.small, medium: SIZES.medium };
  if (process.env.LARGE === "1") sizes.large = SIZES.large;
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
