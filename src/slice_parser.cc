#include "slice_parser.h"
#include "pipeline_metrics.h"
#include <algorithm>
#include <cstring>

namespace ultratab {

void SliceCsvParser::setSelectedColumnIndices(std::vector<std::size_t> indices) {
  selected_column_indices_ = std::move(indices);
}

bool SliceCsvParser::shouldEmitColumn(std::size_t logical_col_idx) const {
  if (selected_column_indices_.empty()) return true;
  return std::find(selected_column_indices_.begin(), selected_column_indices_.end(),
                   logical_col_idx) != selected_column_indices_.end();
}

namespace {

const char CR = '\r';
const char LF = '\n';
inline bool isNewline(char c) { return c == CR || c == LF; }

}  // namespace

SliceCsvParser::SliceCsvParser(const CsvOptions& options)
    : opts_(options), arena_(kArenaBlockSize), batch_size_(options.batch_size) {
  cpu_features_ = detectCpuFeatures();
  current_batch_.reserve(batch_size_);
}

void SliceCsvParser::setMetrics(PipelineMetrics* m) {
  metrics_ = m;
  arena_.setMetrics(m);
}

void SliceCsvParser::feed(const char* seg1, std::size_t len1,
                          const char* seg2, std::size_t len2) {
  seg1_consumed_ = 0;
  seg2_consumed_ = 0;
  processTwoSegments(seg1, len1, seg2, len2, &seg1_consumed_, &seg2_consumed_);
  if (seg1 && len1 > 0 && seg1_consumed_ < len1) {
    remainder_.assign(seg1 + seg1_consumed_, seg1 + len1);
    if (seg2 && len2 > 0)
      remainder_.insert(remainder_.end(), seg2, seg2 + len2);
  } else if (seg2 && len2 > 0 && seg2_consumed_ < len2) {
    remainder_.assign(seg2 + seg2_consumed_, seg2 + len2);
  } else {
    remainder_.clear();
  }
}

void SliceCsvParser::flush() {
  if (state_ == State::InQuoted || state_ == State::InQuotedAfterQuote) {
    return;
  }
  if (!current_row_.empty()) {
    emitRow();
  }
  row_ready_ = false;
  if (!current_batch_.empty()) {
    batch_ready_ = true;
  }
}

void SliceCsvParser::getRemainder(const char** out_ptr, std::size_t* out_len) const {
  if (remainder_.empty()) {
    *out_ptr = nullptr;
    *out_len = 0;
  } else {
    *out_ptr = remainder_.data();
    *out_len = remainder_.size();
  }
}

void SliceCsvParser::skipOneRow() { skip_next_row_ = true; }

SliceBatch SliceCsvParser::takeBatch() {
  batch_ready_ = false;
  SliceBatch out;
  arena_.copyUsedTo(out.arena);
  out.rows = std::move(current_batch_);
  arena_.reset();
  startNewBatch();
  return out;
}

void SliceCsvParser::startNewBatch() {
  current_batch_.clear();
  current_batch_.reserve(batch_size_);
}

void SliceCsvParser::emitField(const char* start, std::size_t len) {
  std::size_t off = arena_.used();
  if (len > 0) {
    arena_.write(start, len);
  }
  current_row_.push_back({off, len});
}

void SliceCsvParser::appendQuoteToLastField() {
  if (current_row_.empty()) return;
  arena_.write(reinterpret_cast<const char*>(&opts_.quote), 1);
  current_row_.back().len += 1;
}

void SliceCsvParser::appendToLastField(const char* start, std::size_t len) {
  if (current_row_.empty() || len == 0) return;
  arena_.write(start, len);
  current_row_.back().len += len;
}

void SliceCsvParser::emitRow() {
  if (skip_next_row_) {
    skip_next_row_ = false;
    current_row_.clear();
    row_ready_ = false;
    logical_column_index_ = 0;
    return;
  }
  row_ready_ = true;
  current_batch_.push_back(std::move(current_row_));
  current_row_.clear();
  logical_column_index_ = 0;
  if (current_batch_.size() >= batch_size_) {
    batch_ready_ = true;
  }
}

void SliceCsvParser::processTwoSegments(const char* p1, std::size_t len1,
                                        const char* p2, std::size_t len2,
                                        std::size_t* consumed1,
                                        std::size_t* consumed2) {
  const char* seg1_end = p1 + len1;
  const char* seg2_start = p2;
  const char* seg2_end = (len2 > 0) ? (p2 + len2) : nullptr;

  const char* cur;
  const char* end;
  bool in_seg1;
  if (len1 > 0) {
    cur = p1;
    end = seg1_end;
    in_seg1 = true;
  } else if (p2 && len2 > 0) {
    cur = p2;
    end = seg2_end;
    in_seg1 = false;
  } else {
    *consumed1 = 0;
    *consumed2 = 0;
    return;
  }
  const char* field_start = cur;
  if (state_ == State::InQuoted && *cur == opts_.quote) {
    field_start = cur + 1;
  }
  bool field_start_in_seg1 = in_seg1;
  (void)field_start_in_seg1;

  auto advance = [&]() {
    ++cur;
    if (in_seg1 && cur >= seg1_end && seg2_start) {
      cur = seg2_start;
      end = seg2_end;
      in_seg1 = false;
    }
  };

  auto copyFieldToArena = [&](const char* from, const char* to) {
    const std::size_t col = logical_column_index_++;
    if (!shouldEmitColumn(col)) return;
    if (from >= to) {
      current_row_.push_back({arena_.used(), 0});
      return;
    }
    std::size_t off = arena_.used();
    std::size_t total = 0;
    if (from < seg1_end) {
      const char* end1 = (to <= seg1_end) ? to : seg1_end;
      std::size_t part1 = static_cast<std::size_t>(end1 - from);
      total += part1;
      arena_.write(from, part1);
    }
    if (seg2_start && to > seg2_start) {
      const char* start2 = (from >= seg2_start) ? from : seg2_start;
      std::size_t part2 = static_cast<std::size_t>(to - start2);
      total += part2;
      arena_.write(start2, part2);
    }
    current_row_.push_back({off, total});
  };

  while (true) {
    if (cur >= end) break;
    char c = *cur;

    switch (state_) {
      case State::FieldStart:
        if (c == opts_.quote) {
          state_ = State::InQuoted;
          advance();
          field_start = cur;
          field_start_in_seg1 = in_seg1;
        } else if (c == opts_.delimiter) {
          copyFieldToArena(cur, cur);
          state_ = State::FieldStart;
          advance();
        } else if (isNewline(c)) {
          copyFieldToArena(cur, cur);
          emitRow();
          state_ = State::FieldStart;
          advance();
          if (cur < end && c == CR && *cur == LF) advance();
          if (batch_ready_) {
            *consumed1 = in_seg1 ? static_cast<std::size_t>(cur - p1) : len1;
            *consumed2 = in_seg1 ? 0 : static_cast<std::size_t>(cur - p2);
            return;
          }
        } else {
          state_ = State::InField;
          field_start = cur;
          field_start_in_seg1 = in_seg1;
          advance();
        }
        break;

      case State::InField: {
        const char* scan_start = cur;
        std::size_t scan_len = static_cast<std::size_t>(end - cur);
        std::size_t sep = scanForSeparator(scan_start, scan_len, opts_.delimiter,
                                          cpu_features_);
        if (sep < scan_len) {
          cur = scan_start + sep;
          in_seg1 = (cur < seg1_end);
          c = *cur;
          if (c == opts_.delimiter) {
            copyFieldToArena(field_start, cur);
            state_ = State::FieldStart;
            advance();
          } else if (isNewline(c)) {
            copyFieldToArena(field_start, cur);
            emitRow();
            state_ = State::FieldStart;
            advance();
            if (cur < end && c == CR && *cur == LF) advance();
            if (batch_ready_) {
              *consumed1 = in_seg1 ? static_cast<std::size_t>(cur - p1) : len1;
              *consumed2 = in_seg1 ? 0 : static_cast<std::size_t>(cur - p2);
              return;
            }
          }
        } else {
          cur = end;
          in_seg1 = false;
        }
        break;
      }

      case State::InQuoted: {
        const char* scan_start = cur;
        std::size_t scan_len = static_cast<std::size_t>(end - cur);
        std::size_t q = scanForChar(scan_start, scan_len, opts_.quote,
                                    cpu_features_);
        if (q < scan_len) {
          cur = scan_start + q;
          in_seg1 = (cur < seg1_end);
          state_ = State::InQuotedAfterQuote;
          advance();
        } else {
          cur = end;
          in_seg1 = false;
        }
        break;
      }

      case State::InQuotedAfterQuote:
        if (c == opts_.quote) {
          copyFieldToArena(field_start, cur);
          appendQuoteToLastField();
          advance();
          field_start = cur;
          field_start_in_seg1 = in_seg1;
          state_ = State::InQuoted;
          after_doubled_quote_ = true;
        } else if (c == opts_.delimiter) {
          if (after_doubled_quote_ && field_start < cur) {
            appendToLastField(field_start, static_cast<std::size_t>(cur - 1 - field_start));
            after_doubled_quote_ = false;
          } else if (field_start < cur) {
            copyFieldToArena(field_start, cur - 1);
          }
          state_ = State::FieldStart;
          advance();
        } else if (isNewline(c)) {
          if (after_doubled_quote_ && field_start < cur) {
            appendToLastField(field_start, static_cast<std::size_t>(cur - 1 - field_start));
            after_doubled_quote_ = false;
          } else if (field_start < cur) {
            copyFieldToArena(field_start, cur - 1);
          }
          emitRow();
          state_ = State::FieldStart;
          advance();
          if (cur < end && c == CR && *cur == LF) advance();
          if (batch_ready_) {
            *consumed1 = in_seg1 ? static_cast<std::size_t>(cur - p1) : len1;
            *consumed2 = in_seg1 ? 0 : static_cast<std::size_t>(cur - p2);
            return;
          }
        } else {
          state_ = State::InField;
          field_start = cur;
          field_start_in_seg1 = in_seg1;
          advance();
        }
        break;
    }
  }

  *consumed1 = len1;
  *consumed2 = (p2 && len2 > 0) ? len2 : 0;
}

}  // namespace ultratab
