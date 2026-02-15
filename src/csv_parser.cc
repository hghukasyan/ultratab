#include "csv_parser.h"
#include "simd_scanner.h"
#include <algorithm>
#include <cstring>

namespace ultratab {

namespace {

const char CR = '\r';
const char LF = '\n';

inline bool isNewline(char c) { return c == CR || c == LF; }

}  // namespace

CsvParser::CsvParser(const CsvOptions& options) : opts_(options) {
  cpu_features_ = detectCpuFeatures();
}

void CsvParser::feed(const char* data, std::size_t len) {
  if (len == 0) {
    if (!remainder_.empty()) processChunk(remainder_.data(), remainder_.size());
    return;
  }
  remainder_.append(data, len);
  processChunk(remainder_.data(), remainder_.size());
}

void CsvParser::feed(const std::string& s) {
  if (s.empty()) return;
  feed(s.data(), s.size());
}

void CsvParser::processChunk(const char* data, std::size_t len) {
  const char* p = data;
  const char* end = data + len;

  while (p < end) {
    char c = *p;

    switch (state_) {
      case State::FieldStart:
        if (c == opts_.quote) {
          state_ = State::InQuoted;
          ++p;
        } else if (c == opts_.delimiter) {
          current_field_.clear();
          current_row_.push_back(std::string());
          state_ = State::FieldStart;
          ++p;
        } else if (isNewline(c)) {
          current_field_.clear();
          current_row_.push_back(std::string());
          emitRow();
          ++p;
          if (p < end && c == CR && *p == LF) ++p;
          if (row_ready_) {
            remainder_ = std::string(p, end);
            return;
          }
        } else {
          state_ = State::InField;
          current_field_.clear();
          current_field_.push_back(c);
          ++p;
        }
        break;

      case State::InField: {
        std::size_t chunk_len = static_cast<std::size_t>(end - p);
        std::size_t sep = scanForSeparator(p, chunk_len, opts_.delimiter,
                                           cpu_features_);
        if (sep < chunk_len) {
          current_field_.append(p, sep);
          p += sep;
          c = *p;
          if (c == opts_.delimiter) {
            current_row_.push_back(std::move(current_field_));
            current_field_.clear();
            state_ = State::FieldStart;
            ++p;
          } else if (isNewline(c)) {
            current_row_.push_back(std::move(current_field_));
            current_field_.clear();
            emitRow();
            state_ = State::FieldStart;
            ++p;
            if (p < end && c == CR && *p == LF) ++p;
            if (row_ready_) {
              remainder_ = std::string(p, end);
              return;
            }
          }
        } else {
          current_field_.append(p, sep);
          p = end;
        }
        break;
      }

      case State::InQuoted: {
        std::size_t chunk_len = static_cast<std::size_t>(end - p);
        std::size_t q = scanForChar(p, chunk_len, opts_.quote, cpu_features_);
        if (q < chunk_len) {
          current_field_.append(p, q);
          p += q;
          state_ = State::InQuotedAfterQuote;
          ++p;
        } else {
          current_field_.append(p, chunk_len);
          p = end;
        }
        break;
      }

      case State::InQuotedAfterQuote:
        if (c == opts_.quote) {
          current_field_.push_back(opts_.quote);
          state_ = State::InQuoted;
          ++p;
        } else if (c == opts_.delimiter) {
          current_row_.push_back(std::move(current_field_));
          current_field_.clear();
          state_ = State::FieldStart;
          ++p;
        } else if (isNewline(c)) {
          current_row_.push_back(std::move(current_field_));
          current_field_.clear();
          emitRow();
          state_ = State::FieldStart;
          ++p;
          if (p < end && c == CR && *p == LF) ++p;
          if (row_ready_) {
            remainder_ = std::string(p, end);
            return;
          }
        } else {
          state_ = State::InField;
          current_field_.push_back(c);
          ++p;
        }
        break;
    }
  }
  remainder_.clear();
}

void CsvParser::emitField() {
  current_row_.push_back(std::move(current_field_));
  current_field_.clear();
}

void CsvParser::emitRow() {
  if (skip_next_row_) {
    skip_next_row_ = false;
    current_row_.clear();
    return;
  }
  pending_row_ = std::move(current_row_);
  current_row_.clear();
  row_ready_ = true;
}

bool CsvParser::hasRow() const { return row_ready_; }

Row CsvParser::takeRow() {
  row_ready_ = false;
  return std::move(pending_row_);
}

bool CsvParser::flush() {
  if (state_ == State::InQuoted || state_ == State::InQuotedAfterQuote) {
    return true;
  }
  if (!current_field_.empty() || !current_row_.empty()) {
    if (!current_field_.empty()) current_row_.push_back(std::move(current_field_));
    pending_row_ = std::move(current_row_);
    current_row_.clear();
    row_ready_ = true;
    return false;
  }
  return false;
}

void CsvParser::skipOneRow() { skip_next_row_ = true; }

}  // namespace ultratab
