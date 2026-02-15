#ifndef ULTRATAB_XLSX_PARSER_H
#define ULTRATAB_XLSX_PARSER_H

#include "columnar_parser.h"
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace ultratab {

struct XlsxOptions {
  int sheet_index = 1;  // 1-based; 0 = use sheet_name
  std::string sheet_name;  // if non-empty, select by name
  bool headers = true;
  std::size_t batch_size = 5000;
  std::vector<std::string> select;
  std::unordered_map<std::string, ColumnType> schema;
  std::vector<std::string> null_values{"", "null", "NULL"};
  bool trim = false;
  TypedFallback typed_fallback = TypedFallback::Null;
};

/// Result for one XLSX batch: either row-based (string[][]) or columnar.
struct XlsxBatch {
  std::vector<std::string> headers;
  bool columnar = false;  // if true, use columns; else use rows
  Batch rows;  // row-based: rows[i][j] = cell string
  ColumnarBatch columnar_batch;  // when columnar && (schema or select)
  std::size_t rowsCount() const {
    return columnar ? columnar_batch.rows : rows.size();
  }
};

/// Load shared strings and resolve sheet path from xlsx at path.
/// Returns (shared_strings, sheet_path). sheet_path is e.g. "xl/worksheets/sheet1.xml".
/// sheet_index is 1-based; sheet_name used if non-empty.
std::pair<std::vector<std::string>, std::string> xlsxResolveSheet(
    const std::string& path,
    int sheet_index,
    const std::string& sheet_name,
    std::string& out_error);

/// Same as xlsxResolveSheet but uses an already-open zip (does not close it).
/// Caller must call mz_zip_reader_end when done.
std::pair<std::vector<std::string>, std::string> xlsxResolveSheetFromZip(
    void* pzip,
    int sheet_index,
    const std::string& sheet_name,
    std::string& out_error);

/// Parse sheet XML (already decompressed) with SAX-style; no DOM.
/// shared_strings: lookup for t="s" cell values.
/// Calls on_row for each row (vector of cell strings). Stops when batch_size rows
/// collected if on_row returns false.
/// Fills sparse cells with empty string up to max column in row.
void xlsxParseSheetXml(
    const char* xml,
    std::size_t xml_len,
    const std::vector<std::string>& shared_strings,
    std::function<bool(std::vector<std::string>&&)> on_row);

/// Convert row-based batch to XlsxBatch (row or columnar per options).
void xlsxBatchFromRows(
    std::vector<std::string>&& headers,
    Batch& rows,
    const XlsxOptions& opts,
    XlsxBatch& out);

}  // namespace ultratab

#endif  // ULTRATAB_XLSX_PARSER_H
