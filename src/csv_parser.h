#ifndef ULTRATAB_CSV_PARSER_H
#define ULTRATAB_CSV_PARSER_H

#include "simd_scanner.h"
#include <cstddef>
#include <string>
#include <vector>

namespace ultratab {

/// Options for CSV parsing (RFC-style).
struct CsvOptions {
  char delimiter = ',';
  char quote = '"';
  bool has_header = false;
  std::size_t batch_size = 10000;
};

/// Single row: vector of field strings.
using Row = std::vector<std::string>;

/// Batch of rows for streaming.
using Batch = std::vector<Row>;

/// Incremental CSV parser: feed chunks, get complete rows.
/// Handles delimiter, quote, escaped quotes, multiline quoted fields.
class CsvParser {
 public:
  explicit CsvParser(const CsvOptions& options);

  /// Feed more data. Call addRow() / flush() after to retrieve rows.
  void feed(const char* data, std::size_t len);

  /// Feed more data from a string.
  void feed(const std::string& s);

  /// Return true if a full row is available; then call takeRow().
  bool hasRow() const;

  /// Take the next complete row. Call only when hasRow() is true.
  Row takeRow();

  /// Call when no more data will be fed. Returns true if a partial row remains.
  bool flush();

  /// Any remaining partial content (incomplete row). Call after last feed() and flush().
  std::string remaining() const { return remainder_; }

  /// Skip one row (e.g. header). Uses same state machine without storing row.
  void skipOneRow();

 private:
  enum class State {
    FieldStart,
    InField,
    InQuoted,
    InQuotedAfterQuote,
  };

  void processChunk(const char* data, std::size_t len);
  void emitField();
  void emitRow();

  CsvOptions opts_;
  CpuFeatures cpu_features_;
  State state_ = State::FieldStart;
  std::string remainder_;
  std::string current_field_;
  Row current_row_;       // row being built
  Row pending_row_;       // completed row waiting for takeRow()
  bool row_ready_ = false;
  bool skip_next_row_ = false;  // for skipOneRow
};

}  // namespace ultratab

#endif  // ULTRATAB_CSV_PARSER_H
