#ifndef ULTRATAB_SLICE_PARSER_H
#define ULTRATAB_SLICE_PARSER_H

#include "arena.h"
#include "csv_parser.h"
#include "simd_scanner.h"
#include <cstddef>
#include <vector>

namespace ultratab {

struct PipelineMetrics;

/// Field slice: (offset, len) into a stable buffer (batch arena).
struct FieldSlice {
  std::size_t offset = 0;
  std::size_t len = 0;
};

/// One row = vector of field slices into the same buffer.
using SliceRow = std::vector<FieldSlice>;

/// A batch of rows; all slices reference arena.data().
struct SliceBatch {
  std::vector<char> arena;
  std::vector<SliceRow> rows;
  std::size_t rowsCount() const { return rows.size(); }
};

/// CSV state machine that operates on byte spans and emits field slices
/// (offset + len) into a per-batch arena. Minimal allocations: one arena per batch.
class SliceCsvParser {
 public:
  explicit SliceCsvParser(const CsvOptions& options);

  /// Feed more data. Segment (ptr, len) valid only during this call.
  /// Optional first segment (remainder from previous feed); use (nullptr, 0) if none.
  void feed(const char* seg1, std::size_t len1, const char* seg2, std::size_t len2);

  /// Call when no more data. Flushes any partial row; remainder() then holds unprocessed.
  void flush();

  /// True if a full batch is available; then call takeBatch().
  bool hasBatch() const { return batch_ready_; }

  /// Take the completed batch. Call only when hasBatch() is true.
  SliceBatch takeBatch();

  /// Unprocessed tail after last feed/flush (for next feed as seg1).
  void getRemainder(const char** out_ptr, std::size_t* out_len) const;

  /// Skip one row (e.g. header). Uses same state machine without storing row.
  void skipOneRow();

  /// Rows accumulated in current incomplete batch (for metrics).
  std::size_t currentBatchRowCount() const { return current_batch_.size(); }

  /// Optional: set to record arena and parse metrics when ULTRATAB_PROFILE is enabled.
  void setMetrics(PipelineMetrics* m);

  /// Arena block size in bytes (1MB–16MB). Used only at construction.
  static constexpr std::size_t kArenaBlockSize = 1024 * 1024;

  /// When non-empty, only these column indices (0-based) are emitted and copied to arena.
  void setSelectedColumnIndices(std::vector<std::size_t> indices);

 private:
  bool shouldEmitColumn(std::size_t logical_col_idx) const;
  enum class State {
    FieldStart,
    InField,
    InQuoted,
    InQuotedAfterQuote,
  };

  void processTwoSegments(const char* p1, std::size_t len1,
                          const char* p2, std::size_t len2,
                          std::size_t* consumed1, std::size_t* consumed2);
  void emitField(const char* start, std::size_t len);
  void appendQuoteToLastField();
  void appendToLastField(const char* start, std::size_t len);
  void emitRow();
  void startNewBatch();

  CsvOptions opts_;
  CpuFeatures cpu_features_;
  State state_ = State::FieldStart;
  std::vector<char> remainder_;
  Arena arena_;
  std::vector<FieldSlice> current_row_;
  std::vector<SliceRow> current_batch_;
  std::size_t batch_size_;
  bool batch_ready_ = false;
  bool skip_next_row_ = false;
  bool row_ready_ = false;

  std::size_t seg1_consumed_ = 0;
  std::size_t seg2_consumed_ = 0;
  PipelineMetrics* metrics_ = nullptr;
  std::vector<std::size_t> selected_column_indices_;
  std::size_t logical_column_index_ = 0;
  bool after_doubled_quote_ = false;
};

}  // namespace ultratab

#endif  // ULTRATAB_SLICE_PARSER_H
