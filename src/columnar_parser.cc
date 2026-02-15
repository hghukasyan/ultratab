#include "columnar_parser.h"
#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>

namespace ultratab {

namespace {

void trimStringImpl(std::string& s) {
  auto start = s.find_first_not_of(" \t\r\n");
  if (start == std::string::npos) {
    s.clear();
    return;
  }
  auto end = s.find_last_not_of(" \t\r\n");
  s = s.substr(start, end == std::string::npos ? std::string::npos : end - start + 1);
}

}  // namespace

void trimString(std::string& s) { trimStringImpl(s); }

bool isNullValue(const std::string& s,
                 const std::vector<std::string>& null_values) {
  for (const auto& nv : null_values) {
    if (s == nv) return true;
  }
  return false;
}

bool parseBool(const char* start, const char* end, bool& out) {
  std::size_t len = static_cast<std::size_t>(end - start);
  if (len == 0) return false;
  if (len == 1) {
    if (*start == '1') { out = true; return true; }
    if (*start == '0') { out = false; return true; }
    return false;
  }
  if (len == 4) {
    if ((start[0] == 't' || start[0] == 'T') &&
        (start[1] == 'r' || start[1] == 'R') &&
        (start[2] == 'u' || start[2] == 'U') &&
        (start[3] == 'e' || start[3] == 'E')) {
      out = true; return true;
    }
  }
  if (len == 5) {
    if ((start[0] == 'f' || start[0] == 'F') &&
        (start[1] == 'a' || start[1] == 'A') &&
        (start[2] == 'l' || start[2] == 'L') &&
        (start[3] == 's' || start[3] == 'S') &&
        (start[4] == 'e' || start[4] == 'E')) {
      out = false; return true;
    }
  }
  return false;
}

bool parseInt32(const char* start, const char* end, std::int32_t& out) {
  if (start >= end) return false;
  const char* p = start;
  bool neg = false;
  if (*p == '-') { neg = true; ++p; }
  else if (*p == '+') { ++p; }
  if (p >= end || !std::isdigit(static_cast<unsigned char>(*p))) return false;

  std::int64_t acc = 0;
  while (p < end && std::isdigit(static_cast<unsigned char>(*p))) {
    acc = acc * 10 + (*p - '0');
    if (acc > 2147483648LL) return false;
    ++p;
  }
  if (p != end) return false;
  if (neg) acc = -acc;
  if (acc < -2147483648LL || acc > 2147483647LL) return false;
  out = static_cast<std::int32_t>(acc);
  return true;
}

bool parseInt64(const char* start, const char* end, std::int64_t& out) {
  if (start >= end) return false;
  const char* p = start;
  bool neg = false;
  if (*p == '-') { neg = true; ++p; }
  else if (*p == '+') { ++p; }
  if (p >= end || !std::isdigit(static_cast<unsigned char>(*p))) return false;

  std::uint64_t acc = 0;
  const std::uint64_t max_val = 9223372036854775808ULL;
  while (p < end && std::isdigit(static_cast<unsigned char>(*p))) {
    if (acc > max_val / 10) return false;
    acc = acc * 10 + static_cast<std::uint64_t>(*p - '0');
    if (acc > max_val) return false;
    ++p;
  }
  if (p != end) return false;
  if (neg) {
    if (acc > max_val) return false;
    out = static_cast<std::int64_t>(-static_cast<std::int64_t>(acc));
  } else {
    if (acc > 9223372036854775807ULL) return false;
    out = static_cast<std::int64_t>(acc);
  }
  return true;
}

bool parseFloat64(const char* start, const char* end, double& out) {
  if (start >= end) return false;
  char* endptr = nullptr;
  double val = std::strtod(start, &endptr);
  if (endptr != end) return false;
  if (std::isnan(val) || std::isinf(val)) return false;
  out = val;
  return true;
}

void rowsToColumnar(const Batch& batch, const std::vector<std::string>& headers,
                    const ColumnarOptions& opts, ColumnarBatch& out) {
  out.rows = batch.size();
  out.columns.clear();

  if (batch.empty()) {
    out.headers.clear();
    return;
  }

  std::unordered_set<std::string> select_set;
  if (!opts.select.empty()) {
    for (const auto& s : opts.select) select_set.insert(s);
  }

  std::vector<std::string> out_headers;
  std::size_t num_cols = headers.size();
  for (std::size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
    const std::string& hdr = headers[col_idx];
    if (!select_set.empty() && select_set.count(hdr) == 0) continue;
    out_headers.push_back(hdr);

    auto it = opts.schema.find(hdr);
    ColumnType col_type = (it != opts.schema.end()) ? it->second : ColumnType::String;

    ColumnarColumn col;
    col.type = col_type;
    bool need_null_mask = (col_type != ColumnType::String);
    if (need_null_mask) {
      col.null_mask = std::make_unique<std::vector<std::uint8_t>>(batch.size(), 0);
    }

    switch (col_type) {
      case ColumnType::String: {
        col.strings.reserve(batch.size());
        for (std::size_t r = 0; r < batch.size(); ++r) {
          std::string cell = (col_idx < batch[r].size()) ? batch[r][col_idx] : "";
          if (opts.trim) trimString(cell);
          if (isNullValue(cell, opts.null_values)) cell = "";
          col.strings.push_back(std::move(cell));
        }
        break;
      }
      case ColumnType::Int32: {
        col.int32_data = std::make_unique<std::vector<std::int32_t>>(batch.size(), 0);
        for (std::size_t r = 0; r < batch.size(); ++r) {
          std::string cell = (col_idx < batch[r].size()) ? batch[r][col_idx] : "";
          if (opts.trim) trimString(cell);
          if (isNullValue(cell, opts.null_values)) {
            (*col.null_mask)[r] = 1;
            continue;
          }
          std::int32_t v;
          if (parseInt32(cell.data(), cell.data() + cell.size(), v)) {
            (*col.int32_data)[r] = v;
          } else {
            if (opts.typed_fallback == TypedFallback::Null) {
              (*col.null_mask)[r] = 1;
            } else {
              (*col.null_mask)[r] = 1;  // Still mark as null for typed column; fallback "string" would require different storage
              // For typed columns, fallback to null when parse fails (simplest).
            }
          }
        }
        break;
      }
      case ColumnType::Int64: {
        col.int64_data = std::make_unique<std::vector<std::int64_t>>(batch.size(), 0);
        for (std::size_t r = 0; r < batch.size(); ++r) {
          std::string cell = (col_idx < batch[r].size()) ? batch[r][col_idx] : "";
          if (opts.trim) trimString(cell);
          if (isNullValue(cell, opts.null_values)) {
            (*col.null_mask)[r] = 1;
            continue;
          }
          std::int64_t v;
          if (parseInt64(cell.data(), cell.data() + cell.size(), v)) {
            (*col.int64_data)[r] = v;
          } else {
            (*col.null_mask)[r] = 1;
          }
        }
        break;
      }
      case ColumnType::Float64: {
        col.float64_data = std::make_unique<std::vector<double>>(batch.size(), 0.0);
        for (std::size_t r = 0; r < batch.size(); ++r) {
          std::string cell = (col_idx < batch[r].size()) ? batch[r][col_idx] : "";
          if (opts.trim) trimString(cell);
          if (isNullValue(cell, opts.null_values)) {
            (*col.null_mask)[r] = 1;
            continue;
          }
          double v;
          if (parseFloat64(cell.data(), cell.data() + cell.size(), v)) {
            (*col.float64_data)[r] = v;
          } else {
            (*col.null_mask)[r] = 1;
          }
        }
        break;
      }
      case ColumnType::Bool: {
        col.bool_data = std::make_unique<std::vector<std::uint8_t>>(batch.size(), 0);
        for (std::size_t r = 0; r < batch.size(); ++r) {
          std::string cell = (col_idx < batch[r].size()) ? batch[r][col_idx] : "";
          if (opts.trim) trimString(cell);
          if (isNullValue(cell, opts.null_values)) {
            (*col.null_mask)[r] = 1;
            continue;
          }
          bool v;
          if (parseBool(cell.data(), cell.data() + cell.size(), v)) {
            (*col.bool_data)[r] = v ? 1 : 0;
          } else {
            (*col.null_mask)[r] = 1;
          }
        }
        break;
      }
    }

    out.columns[hdr] = std::move(col);
  }
  out.headers = std::move(out_headers);
}

}  // namespace ultratab
