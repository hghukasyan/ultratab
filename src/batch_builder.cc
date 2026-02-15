#include "batch_builder.h"
#include <cstring>
#include <string>

namespace ultratab {

namespace {

std::string sliceToStr(const FieldSlice& s, const char* arena_data,
                       std::size_t arena_size) {
  if (s.offset >= arena_size || s.len == 0) return "";
  std::size_t end = s.offset + s.len;
  if (end > arena_size) end = arena_size;
  return std::string(arena_data + s.offset, end - s.offset);
}

}  // namespace

std::vector<std::string> sliceRowToStrings(const SliceRow& row,
                                            const char* arena_data,
                                            std::size_t arena_size) {
  std::vector<std::string> out;
  out.reserve(row.size());
  for (const auto& s : row) {
    out.push_back(sliceToStr(s, arena_data, arena_size));
  }
  return out;
}

void buildRowBatch(const SliceBatch& slice_batch, Batch& out) {
  out.clear();
  const char* arena = slice_batch.arena.data();
  std::size_t arena_size = slice_batch.arena.size();
  out.reserve(slice_batch.rows.size());
  for (const auto& row : slice_batch.rows) {
    out.push_back(sliceRowToStrings(row, arena, arena_size));
  }
}

void buildColumnarBatch(const SliceBatch& slice_batch,
                        const std::vector<std::string>& headers,
                        const ColumnarOptions& options,
                        ColumnarBatch& out) {
  const char* arena = slice_batch.arena.data();
  std::size_t arena_size = slice_batch.arena.size();
  Batch row_batch;
  row_batch.reserve(slice_batch.rows.size());
  for (const auto& row : slice_batch.rows) {
    row_batch.push_back(sliceRowToStrings(row, arena, arena_size));
  }
  rowsToColumnar(row_batch, headers, options, out);
}

}  // namespace ultratab
