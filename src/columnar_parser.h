#ifndef ULTRATAB_COLUMNAR_PARSER_H
#define ULTRATAB_COLUMNAR_PARSER_H

#include "csv_parser.h"
#include <cstdint>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ultratab {

enum class ColumnType { String, Int32, Int64, Float64, Bool };

enum class TypedFallback { String, Null };

struct ColumnarOptions {
  char delimiter = ',';
  char quote = '"';
  bool has_header = true;
  std::size_t batch_size = 10000;
  std::vector<std::string> select;   // empty = all columns
  std::unordered_map<std::string, ColumnType> schema;
  std::vector<std::string> null_values{"", "null", "NULL"};
  bool trim = false;
  TypedFallback typed_fallback = TypedFallback::Null;
};

struct ColumnarColumn {
  ColumnType type = ColumnType::String;
  std::vector<std::string> strings;
  std::unique_ptr<std::vector<std::int32_t>> int32_data;
  std::unique_ptr<std::vector<std::int64_t>> int64_data;
  std::unique_ptr<std::vector<double>> float64_data;
  std::unique_ptr<std::vector<std::uint8_t>> bool_data;
  std::unique_ptr<std::vector<std::uint8_t>> null_mask;
};

struct ColumnarBatch {
  std::vector<std::string> headers;
  std::unordered_map<std::string, ColumnarColumn> columns;
  std::size_t rows = 0;
};

/// Convert row-based batch to columnar. Headers must match row column count.
void rowsToColumnar(const Batch& batch, const std::vector<std::string>& headers,
                    const ColumnarOptions& opts, ColumnarBatch& out);

/// Check if string is null per null_values.
bool isNullValue(const std::string& s, const std::vector<std::string>& null_values);

/// Trim leading/trailing whitespace in place.
void trimString(std::string& s);

/// Fast parseInt32. Returns true on success. No locale.
bool parseInt32(const char* start, const char* end, std::int32_t& out);

/// Fast parseInt64. Returns true on success. No locale.
bool parseInt64(const char* start, const char* end, std::int64_t& out);

/// Fast parseDouble. Returns true on success. Handles sign, decimal, exponent.
bool parseFloat64(const char* start, const char* end, double& out);

/// Fast parseBool. Accepts "true","false","1","0" (case-insensitive).
bool parseBool(const char* start, const char* end, bool& out);

}  // namespace ultratab

#endif  // ULTRATAB_COLUMNAR_PARSER_H
