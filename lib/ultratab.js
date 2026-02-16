"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path = require("path");
const fs = require("fs");
const pkgRoot = path.resolve(__dirname, "..");
function loadAddon() {
    const candidates = [
        path.join(pkgRoot, "build", "Release", "ultratab.node"),
        path.join(pkgRoot, "build", "Debug", "ultratab.node"),
    ];
    for (const candidate of candidates) {
        try {
            if (fs.existsSync(candidate)) {
                return require(candidate);
            }
        }
        catch {
            continue;
        }
    }
    const err = new Error("ultratab: Native addon not found. Run `npm run build` to compile the addon. " +
        "If you installed from npm, ensure CMake and a C++17 compiler are available, then run `npm rebuild ultratab`.");
    err.code = "ULTRATAB_ADDON_NOT_FOUND";
    throw err;
}
const addon = loadAddon();
function csv(filePath, options) {
    if (typeof filePath !== "string") {
        throw new TypeError("csv(): path must be a string");
    }
    const parser = addon.createParser(filePath, options || {});
    if (!parser) {
        throw new Error("csv(): failed to create parser");
    }
    let destroyed = false;
    function destroy() {
        if (destroyed)
            return;
        destroyed = true;
        addon.destroyParser(parser);
    }
    return {
        [Symbol.asyncIterator]() {
            return {
                async next() {
                    if (destroyed) {
                        return { value: undefined, done: true };
                    }
                    const value = await addon.getNextBatch(parser);
                    if (value === undefined) {
                        destroy();
                        return { value: undefined, done: true };
                    }
                    return { value, done: false };
                },
                async return() {
                    destroy();
                    return { value: undefined, done: true };
                },
            };
        },
    };
}
function csvColumns(filePath, options) {
    if (typeof filePath !== "string") {
        throw new TypeError("csvColumns(): path must be a string");
    }
    const parser = addon.createColumnarParser(filePath, options || {});
    if (!parser) {
        throw new Error("csvColumns(): failed to create parser");
    }
    let destroyed = false;
    function destroy() {
        if (destroyed)
            return;
        destroyed = true;
        addon.destroyColumnarParser(parser);
    }
    return {
        [Symbol.asyncIterator]() {
            return {
                async next() {
                    if (destroyed) {
                        return { value: undefined, done: true };
                    }
                    const value = await addon.getNextColumnarBatch(parser);
                    if (value === undefined) {
                        destroy();
                        return { value: undefined, done: true };
                    }
                    return { value, done: false };
                },
                async return() {
                    destroy();
                    return { value: undefined, done: true };
                },
            };
        },
    };
}
function xlsx(filePath, options) {
    if (typeof filePath !== "string") {
        throw new TypeError("xlsx(): path must be a string");
    }
    const parser = addon.createXlsxParser(filePath, options || {});
    if (!parser) {
        throw new Error("xlsx(): failed to create parser");
    }
    let destroyed = false;
    function destroy() {
        if (destroyed)
            return;
        destroyed = true;
        addon.destroyXlsxParser(parser);
    }
    return {
        [Symbol.asyncIterator]() {
            return {
                async next() {
                    if (destroyed) {
                        return { value: undefined, done: true };
                    }
                    const value = await addon.getNextXlsxBatch(parser);
                    if (value === undefined) {
                        destroy();
                        return { value: undefined, done: true };
                    }
                    return { value, done: false };
                },
                async return() {
                    destroy();
                    return { value: undefined, done: true };
                },
            };
        },
    };
}
function getParserMetrics(parser) {
    if (!parser)
        return null;
    return addon.getParserMetrics?.(parser) ?? null;
}
function getColumnarParserMetrics(parser) {
    if (!parser)
        return null;
    return addon.getColumnarParserMetrics?.(parser) ?? null;
}
module.exports = {
    csv,
    csvColumns,
    xlsx,
    getParserMetrics,
    getColumnarParserMetrics,
    createParser: (p, opts) => addon.createParser(p, opts),
    getNextBatch: (parser) => addon.getNextBatch(parser),
    destroyParser: (parser) => addon.destroyParser(parser),
    createColumnarParser: (p, opts) => addon.createColumnarParser(p, opts),
    getNextColumnarBatch: (parser) => addon.getNextColumnarBatch(parser),
    destroyColumnarParser: (parser) => addon.destroyColumnarParser(parser),
};
