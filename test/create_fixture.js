"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * Creates a minimal valid .xlsx (ZIP) fixture for tests.
 * No external deps; uses Node zlib + manual ZIP layout.
 */
const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const fixturePath = path.join(__dirname, "fixture.xlsx");
// Minimal OOXML parts
const contentTypes = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
<Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>
<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
<Override PartName="/xl/_rels/workbook.xml.rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
</Types>`;
const workbookRels = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>
<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>`;
const workbook = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets></workbook>`;
const sharedStrings = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="4" uniqueCount="3">
<si><t>Name</t></si><si><t>Value</t></si><si><t>Hello</t></si><si><t>World</t></si></sst>`;
const styles = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="1"/><fills count="1"/><borders count="1"/><cellStyleXfs count="1"/><cellXfs count="1"/></styleSheet>`;
const sheet1 = `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<sheetData>
<row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>
<row r="2"><c r="A2" t="s"><v>2</v></c><c r="B2"><v>1</v></c></row>
<row r="3"><c r="A3" t="s"><v>3</v></c><c r="B3"><v>2</v></c></row>
</sheetData></worksheet>`;
const entries = [
    ["[Content_Types].xml", contentTypes],
    ["xl/_rels/workbook.xml.rels", workbookRels],
    ["xl/workbook.xml", workbook],
    ["xl/sharedStrings.xml", sharedStrings],
    ["xl/styles.xml", styles],
    ["xl/worksheets/sheet1.xml", sheet1],
];
function toBuffer(s) {
    return Buffer.from(s, "utf8");
}
const crc32Table = (() => {
    const t = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
        let c = n;
        for (let k = 0; k < 8; k++)
            c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
        t[n] = c;
    }
    return t;
})();
function crc32(buf) {
    let c = 0 ^ -1;
    for (let i = 0; i < buf.length; i++) {
        c = crc32Table[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
    }
    return (c ^ -1) >>> 0;
}
function writeU16(b, o, v) {
    b[o] = v & 0xff;
    b[o + 1] = (v >> 8) & 0xff;
}
function writeU32(b, o, v) {
    b[o] = v & 0xff;
    b[o + 1] = (v >> 8) & 0xff;
    b[o + 2] = (v >> 16) & 0xff;
    b[o + 3] = (v >> 24) & 0xff;
}
const LOCAL_HEADER_SIG = 0x04034b50;
const CENTRAL_SIG = 0x02014b50;
const EOCD_SIG = 0x06054b50;
const chunks = [];
const centralRecords = [];
let offset = 0;
for (const [name, content] of entries) {
    const uncompressed = toBuffer(content);
    const compressed = zlib.deflateSync(uncompressed, { level: 9 });
    const nameBuf = Buffer.from(name, "utf8");
    const localHeaderLen = 30 + nameBuf.length;
    const localHeader = Buffer.alloc(localHeaderLen);
    let o = 0;
    writeU32(localHeader, o, LOCAL_HEADER_SIG);
    o += 4;
    writeU16(localHeader, o, 20);
    o += 2;
    writeU16(localHeader, o, 0);
    o += 2;
    writeU16(localHeader, o, 8);
    o += 2;
    writeU16(localHeader, o, 0);
    writeU16(localHeader, o + 2, 0);
    o += 4;
    writeU32(localHeader, o, 0);
    o += 4;
    writeU32(localHeader, o, compressed.length);
    o += 4;
    writeU32(localHeader, o, uncompressed.length);
    o += 4;
    writeU16(localHeader, o, nameBuf.length);
    o += 2;
    writeU16(localHeader, o, 0);
    o += 2;
    nameBuf.copy(localHeader, o);
    chunks.push(localHeader, compressed);
    const centralLen = 46 + nameBuf.length;
    const central = Buffer.alloc(centralLen);
    o = 0;
    writeU32(central, o, CENTRAL_SIG);
    o += 4;
    writeU16(central, o, 20);
    o += 2;
    writeU16(central, o, 20);
    o += 2;
    writeU16(central, o, 0);
    o += 2;
    writeU16(central, o, 8);
    o += 2;
    writeU16(central, o, 0);
    writeU16(central, o + 2, 0);
    o += 4;
    writeU32(central, o, crc32(uncompressed));
    o += 4;
    writeU32(central, o, compressed.length);
    o += 4;
    writeU32(central, o, uncompressed.length);
    o += 4;
    writeU16(central, o, nameBuf.length);
    o += 2;
    writeU16(central, o, 0);
    o += 2;
    writeU16(central, o, 0);
    o += 2;
    writeU16(central, o, 0);
    o += 2;
    writeU32(central, o, 0);
    o += 4;
    writeU32(central, o, offset);
    o += 4;
    nameBuf.copy(central, o);
    centralRecords.push({ record: central, offset });
    offset += localHeaderLen + compressed.length;
}
const centralStart = offset;
let centralSize = 0;
for (const { record } of centralRecords) {
    chunks.push(record);
    centralSize += record.length;
    offset += record.length;
}
const eocdLen = 22;
const eocd = Buffer.alloc(eocdLen);
let eo = 0;
writeU32(eocd, eo, EOCD_SIG);
eo += 4;
writeU16(eocd, eo, 0);
eo += 2;
writeU16(eocd, eo, 0);
eo += 2;
writeU16(eocd, eo, entries.length);
eo += 2;
writeU16(eocd, eo, entries.length);
eo += 2;
writeU32(eocd, eo, centralSize);
eo += 4;
writeU32(eocd, eo, centralStart);
eo += 4;
writeU16(eocd, eo, 0);
eo += 2;
chunks.push(eocd);
fs.writeFileSync(fixturePath, Buffer.concat(chunks));
console.log("Wrote", fixturePath);
process.exit(0);
