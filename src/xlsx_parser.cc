#include "xlsx_parser.h"
#include <algorithm>
#include <cctype>
#include <cstring>
#include <sstream>

extern "C" {
#include "miniz.h"
#include "miniz_zip.h"
}

namespace ultratab {

namespace {

const char* const kWorkbookRels = "xl/_rels/workbook.xml.rels";
const char* const kWorkbook = "xl/workbook.xml";
const char* const kSharedStrings = "xl/sharedStrings.xml";

// ---- Lightweight XML helpers (no DOM, just tag/attr/text) ----

struct XmlCursor {
  const char* p;
  const char* end;
  bool eof() const { return p >= end; }
  void skipWs() {
    while (p < end && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) ++p;
  }
  bool consume(const char* s) {
    size_t n = std::strlen(s);
    if (static_cast<size_t>(end - p) < n) return false;
    if (std::memcmp(p, s, n) != 0) return false;
    p += n;
    return true;
  }
  bool peekOpenTag(const char* tag, const char*& attrStart) {
    skipWs();
    if (p >= end || *p != '<') return false;
    ++p;
    skipWs();
    size_t tagLen = std::strlen(tag);
    if (static_cast<size_t>(end - p) < tagLen) return false;
    if (std::memcmp(p, tag, tagLen) != 0) return false;
    p += tagLen;
    skipWs();
    attrStart = p;
    return true;
  }
  bool getAttr(const char* attr, std::string& value) {
    const char* start = p;
    while (start < end && *start != '>' && *start != '/') {
      if (*start == ' ' || *start == '\t') {
        ++start;
        continue;
      }
      size_t attrLen = std::strlen(attr);
      if (static_cast<size_t>(end - start) >= attrLen + 2 &&
          std::memcmp(start, attr, attrLen) == 0 && start[attrLen] == '=') {
        char quote = start[attrLen + 1];
        if (quote != '"' && quote != '\'') return false;
        const char* vStart = start + attrLen + 2;
        const char* vEnd = vStart;
        while (vEnd < end && *vEnd != quote) ++vEnd;
        value.assign(vStart, vEnd - vStart);
        p = vEnd + 1;
        return true;
      }
      while (start < end && *start != ' ' && *start != '\t' && *start != '>' && *start != '=') ++start;
      if (start < end && *start == '=') {
        ++start;
        if (start < end && (*start == '"' || *start == '\'')) {
          char q = *start++;
          while (start < end && *start != q) ++start;
          if (start < end) ++start;
        }
      }
    }
    return false;
  }
  void skipToClose(const char* tag) {
    size_t tagLen = std::strlen(tag);
    while (p < end) {
      if (*p == '<') {
        if (p + 1 < end && p[1] == '/') {
          if (static_cast<size_t>(end - p) >= 2 + tagLen && p[2] == tag[0] &&
              (tagLen == 1 || std::memcmp(p + 2, tag, tagLen) == 0)) {
            if (p[2 + tagLen] == '>' || std::isspace(static_cast<unsigned char>(p[2 + tagLen]))) {
              p += 2 + tagLen;
              while (p < end && *p != '>') ++p;
              if (p < end) ++p;
              return;
            }
          }
        }
        ++p;
        continue;
      }
      ++p;
    }
  }
};

// Parse workbook.xml.rels: Relationship Id="rId1" Target="worksheets/sheet1.xml"
void parseWorkbookRels(const char* data, size_t len,
                       std::unordered_map<std::string, std::string>& idToTarget) {
  XmlCursor cur{data, data + len};
  idToTarget.clear();
  for (;;) {
    cur.skipWs();
    if (cur.eof()) break;
    if (!cur.consume("<Relationship")) {
      ++cur.p;
      continue;
    }
    std::string id, target;
    const char* attrStart = cur.p;
    while (cur.p < cur.end && *cur.p != '>' && *cur.p != '/') {
      cur.p = attrStart;
      if (cur.getAttr("Id", id)) { attrStart = cur.p; continue; }
      if (cur.getAttr("Target", target)) { attrStart = cur.p; continue; }
      while (attrStart < cur.end && *attrStart != ' ' && *attrStart != '\t' && *attrStart != '>' && *attrStart != '/') ++attrStart;
      if (attrStart >= cur.end) break;
      cur.p = attrStart;
    }
    if (!id.empty() && !target.empty()) {
      if (target[0] != '/') target = "xl/" + target;
      else target = "xl" + target;
      idToTarget[id] = std::move(target);
    }
    cur.skipToClose("Relationship");
  }
}

// Parse workbook.xml: <sheet name="Sheet1" sheetId="1" r:id="rId1"/>
void parseWorkbookSheets(const char* data, size_t len,
                         const std::unordered_map<std::string, std::string>& idToTarget,
                         std::vector<std::pair<std::string, std::string>>& sheets) {
  sheets.clear();
  XmlCursor cur{data, data + len};
  for (;;) {
    cur.skipWs();
    if (cur.eof()) break;
    if (*cur.p != '<') { ++cur.p; continue; }
    if (cur.p + 5 < cur.end && cur.p[1] == 's' && cur.p[2] == 'h' && cur.p[3] == 'e' && cur.p[4] == 'e' && cur.p[5] == 't') {
      cur.p += 1 + 5;
      cur.skipWs();
      std::string name, rid;
      while (cur.p < cur.end && *cur.p != '>' && *cur.p != '/') {
        if (cur.getAttr("name", name)) continue;
        if (cur.getAttr("r:id", rid) || cur.getAttr("id", rid)) continue;
        while (cur.p < cur.end && *cur.p != ' ' && *cur.p != '\t' && *cur.p != '>' && *cur.p != '=') ++cur.p;
        if (cur.p < cur.end && *cur.p == '=') {
          ++cur.p;
          if (cur.p < cur.end && (*cur.p == '"' || *cur.p == '\'')) {
            char q = *cur.p++;
            while (cur.p < cur.end && *cur.p != q) ++cur.p;
            if (cur.p < cur.end) ++cur.p;
          }
        }
      }
      auto it = idToTarget.find(rid);
      if (it != idToTarget.end())
        sheets.push_back({name, it->second});
      cur.skipToClose("sheet");
      continue;
    }
    ++cur.p;
  }
}

// Parse sharedStrings.xml: <si><t>text</t></si> or <si><r><t>a</t></r><r><t>b</t></r></si>
void parseSharedStrings(const char* data, size_t len, std::vector<std::string>& out) {
  out.clear();
  XmlCursor cur{data, data + len};
  for (;;) {
    cur.skipWs();
    if (cur.eof()) break;
    if (cur.p + 3 <= cur.end && cur.p[0] == '<' && cur.p[1] == 's' && cur.p[2] == 'i') {
      cur.p += 3;
      if (cur.p < cur.end && *cur.p == '>') ++cur.p;
      std::string item;
      while (cur.p < cur.end) {
        cur.skipWs();
        if (cur.p + 2 <= cur.end && cur.p[0] == '<' && cur.p[1] == '/') {
          if (cur.p[2] == 's' && cur.p[3] == 'i') break;
          cur.skipToClose("si");
          break;
        }
        if (cur.p + 2 <= cur.end && cur.p[0] == '<' && cur.p[1] == 't') {
          cur.p += 2;
          while (cur.p < cur.end && *cur.p != '>') ++cur.p;
          if (cur.p < cur.end) ++cur.p;
          const char* start = cur.p;
          while (cur.p < cur.end && *cur.p != '<') ++cur.p;
          item.append(start, cur.p - start);
          cur.skipToClose("t");
          continue;
        }
        if (cur.p + 2 <= cur.end && cur.p[0] == '<' && cur.p[1] == 'r') {
          cur.p += 2;
          while (cur.p < cur.end && *cur.p != '>') ++cur.p;
          if (cur.p < cur.end) ++cur.p;
          cur.skipWs();
          if (cur.p + 2 <= cur.end && cur.p[0] == '<' && cur.p[1] == 't') {
            cur.p += 2;
            while (cur.p < cur.end && *cur.p != '>') ++cur.p;
            if (cur.p < cur.end) ++cur.p;
            const char* start = cur.p;
            while (cur.p < cur.end && *cur.p != '<') ++cur.p;
            item.append(start, cur.p - start);
            cur.skipToClose("t");
          }
          cur.skipToClose("r");
          continue;
        }
        ++cur.p;
      }
      out.push_back(std::move(item));
      cur.skipToClose("si");
      continue;
    }
    ++cur.p;
  }
}

// A1 -> 0, B2 -> 1, BC23 -> 54 (0-based column)
int cellRefToCol(const char* ref, const char* end) {
  const char* p = ref;
  while (p < end && std::isdigit(static_cast<unsigned char>(*p))) ++p;
  if (p == ref) return -1;
  int col = 0;
  for (const char* q = ref; q < p; ++q) {
    char c = *q;
    if (c >= 'A' && c <= 'Z') col = col * 26 + (c - 'A' + 1);
    else if (c >= 'a' && c <= 'z') col = col * 26 + (c - 'a' + 1);
  }
  return col > 0 ? col - 1 : -1;
}

}  // namespace

std::pair<std::vector<std::string>, std::string> xlsxResolveSheet(
    const std::string& path,
    int sheet_index,
    const std::string& sheet_name,
    std::string& out_error) {
  out_error.clear();
  mz_zip_archive zip;
  mz_zip_zero_struct(&zip);
  if (!mz_zip_reader_init_file(&zip, path.c_str(), 0)) {
    out_error = "Failed to open XLSX (ZIP): ";
    out_error += path;
    return {{}, ""};
  }
  auto result = xlsxResolveSheetFromZip(&zip, sheet_index, sheet_name, out_error);
  mz_zip_reader_end(&zip);
  return result;
}

std::pair<std::vector<std::string>, std::string> xlsxResolveSheetFromZip(
    void* pzip,
    int sheet_index,
    const std::string& sheet_name,
    std::string& out_error) {
  out_error.clear();
  mz_zip_archive* zip = static_cast<mz_zip_archive*>(pzip);

  int relsIdx = mz_zip_reader_locate_file(zip, kWorkbookRels, nullptr, 0);
  if (relsIdx < 0) {
    out_error = "XLSX: missing xl/_rels/workbook.xml.rels";
    return {{}, ""};
  }
  size_t relsSize = 0;
  void* relsBuf = mz_zip_reader_extract_to_heap(zip, relsIdx, &relsSize, 0);
  if (!relsBuf) {
    out_error = "XLSX: failed to read workbook.xml.rels";
    return {{}, ""};
  }
  std::unordered_map<std::string, std::string> idToTarget;
  parseWorkbookRels(static_cast<const char*>(relsBuf), relsSize, idToTarget);
  mz_free(relsBuf);

  int wbIdx = mz_zip_reader_locate_file(zip, kWorkbook, nullptr, 0);
  if (wbIdx < 0) {
    out_error = "XLSX: missing xl/workbook.xml";
    return {{}, ""};
  }
  size_t wbSize = 0;
  void* wbBuf = mz_zip_reader_extract_to_heap(zip, wbIdx, &wbSize, 0);
  if (!wbBuf) {
    out_error = "XLSX: failed to read workbook.xml";
    return {{}, ""};
  }
  std::vector<std::pair<std::string, std::string>> sheets;
  parseWorkbookSheets(static_cast<const char*>(wbBuf), wbSize, idToTarget, sheets);
  mz_free(wbBuf);

  if (sheets.empty()) {
    out_error = "XLSX: no sheets in workbook";
    return {{}, ""};
  }

  std::string sheetPath;
  if (!sheet_name.empty()) {
    for (const auto& s : sheets) {
      if (s.first == sheet_name) { sheetPath = s.second; break; }
    }
    if (sheetPath.empty()) {
      out_error = "XLSX: sheet not found: ";
      out_error += sheet_name;
      return {{}, ""};
    }
  } else {
    int idx = sheet_index >= 1 ? sheet_index - 1 : 0;
    if (idx < 0 || static_cast<size_t>(idx) >= sheets.size()) {
      out_error = "XLSX: sheet index out of range";
      return {{}, ""};
    }
    sheetPath = sheets[static_cast<size_t>(idx)].second;
  }

  std::vector<std::string> shared_strings;
  int ssIdx = mz_zip_reader_locate_file(zip, kSharedStrings, nullptr, 0);
  if (ssIdx >= 0) {
    size_t ssSize = 0;
    void* ssBuf = mz_zip_reader_extract_to_heap(zip, ssIdx, &ssSize, 0);
    if (ssBuf) {
      parseSharedStrings(static_cast<const char*>(ssBuf), ssSize, shared_strings);
      mz_free(ssBuf);
    }
  }

  return {std::move(shared_strings), std::move(sheetPath)};
}

void xlsxParseSheetXml(
    const char* xml,
    std::size_t xml_len,
    const std::vector<std::string>& shared_strings,
    std::function<bool(std::vector<std::string>&&)> on_row) {
  XmlCursor cur{xml, xml + xml_len};
  bool inSheetData = false;
  std::vector<std::pair<int, std::string>> cellList;
  int maxCol = -1;
  while (!cur.eof()) {
    cur.skipWs();
    if (cur.eof()) break;
    if (*cur.p != '<') { ++cur.p; continue; }

    // <row ...>
    if (cur.p + 4 <= cur.end && cur.p[1] == 'r' && cur.p[2] == 'o' && cur.p[3] == 'w') {
      cur.p += 4;
      while (cur.p < cur.end && *cur.p != '>' && *cur.p != '/') ++cur.p;
      if (cur.p < cur.end && *cur.p == '>') ++cur.p;
      inSheetData = true;
      cellList.clear();
      maxCol = -1;
      continue;
    }

    // </row>
    if (cur.p + 6 <= cur.end && cur.p[1] == '/' && cur.p[2] == 'r' && cur.p[3] == 'o' && cur.p[4] == 'w') {
      cur.p += 6;
      while (cur.p < cur.end && *cur.p != '>') ++cur.p;
      if (cur.p < cur.end) ++cur.p;
      if (!cellList.empty() || maxCol >= 0) {
        int cols = maxCol + 1;
        if (cols < 0) cols = 0;
        std::vector<std::string> row(static_cast<size_t>(cols), "");
        for (const auto& c : cellList) {
          if (c.first >= 0 && c.first < cols)
            row[static_cast<size_t>(c.first)] = c.second;
        }
        if (!on_row(std::move(row))) return;
      }
      inSheetData = false;
      continue;
    }

    // <c r="A1" t="s"> or <c r="B2">
    if (inSheetData && cur.p + 2 <= cur.end && cur.p[1] == 'c') {
      const char* tagStart = cur.p;
      cur.p += 2;
      while (cur.p < cur.end && *cur.p != '>' && *cur.p != '/') ++cur.p;
      const char* attrStart = tagStart + 2;
      XmlCursor attrCur{attrStart, cur.p};
      std::string r, t;
      attrCur.getAttr("r", r);
      attrCur.getAttr("t", t);
      int col = cellRefToCol(r.data(), r.data() + r.size());
      if (col < 0) { cur.skipToClose("c"); continue; }
      if (col > maxCol) maxCol = col;

      std::string value;
      // <v>123</v> or <is><t>inline</t></is>
      cur.skipWs();
      if (cur.p + 2 <= cur.end && cur.p[0] == '<' && cur.p[1] == 'v') {
        cur.p += 2;
        while (cur.p < cur.end && *cur.p != '>') ++cur.p;
        if (cur.p < cur.end) ++cur.p;
        const char* vStart = cur.p;
        while (cur.p < cur.end && *cur.p != '<') ++cur.p;
        value.assign(vStart, cur.p - vStart);
        cur.skipToClose("v");
      } else if (cur.p + 3 <= cur.end && cur.p[0] == '<' && cur.p[1] == 'i' && cur.p[2] == 's') {
        cur.p += 3;
        while (cur.p < cur.end && *cur.p != '>') ++cur.p;
        if (cur.p < cur.end) ++cur.p;
        cur.skipWs();
        if (cur.p + 2 <= cur.end && cur.p[0] == '<' && cur.p[1] == 't') {
          cur.p += 2;
          while (cur.p < cur.end && *cur.p != '>') ++cur.p;
          if (cur.p < cur.end) ++cur.p;
          const char* vStart = cur.p;
          while (cur.p < cur.end && *cur.p != '<') ++cur.p;
          value.assign(vStart, cur.p - vStart);
        }
        cur.skipToClose("is");
      }

      if (t == "s") {
        // shared string index
        int idx = -1;
        try {
          idx = std::stoi(value);
        } catch (...) {}
        if (idx >= 0 && static_cast<size_t>(idx) < shared_strings.size())
          value = shared_strings[static_cast<size_t>(idx)];
      } else if (t == "b") {
        if (value == "1" || value == "true" || value == "TRUE") value = "true";
        else value = "false";
      }
      cellList.push_back({col, std::move(value)});
      continue;
    }

    ++cur.p;
  }
}

void xlsxBatchFromRows(
    std::vector<std::string>&& headers,
    Batch& rows,
    const XlsxOptions& opts,
    XlsxBatch& out) {
  out.headers = std::move(headers);
  out.columnar = !opts.schema.empty() || !opts.select.empty();
  if (out.columnar) {
    ColumnarOptions co;
    co.has_header = true;
    co.batch_size = opts.batch_size;
    co.select = opts.select;
    co.schema = opts.schema;
    co.null_values = opts.null_values;
    co.trim = opts.trim;
    co.typed_fallback = opts.typed_fallback;
    rowsToColumnar(rows, out.headers, co, out.columnar_batch);
    out.columnar_batch.headers = out.headers;
  } else {
    out.rows = std::move(rows);
  }
}

}  // namespace ultratab
