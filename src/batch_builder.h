#ifndef ULTRATAB_BATCH_BUILDER_H
#define ULTRATAB_BATCH_BUILDER_H

#include "columnar_parser.h"
#include "slice_parser.h"
#include "csv_parser.h"
#include <cstddef>

namespace ultratab {

/// Build row-based Batch from SliceBatch. Copies slice data to strings only when
/// building; arena referenced by slices must stay valid during build().
void buildRowBatch(const SliceBatch& slice_batch, Batch& out);

/// Build columnar ColumnarBatch from SliceBatch. Uses arena for string views;
/// typed columns parsed in place. Headers and options for schema/select/null/trim.
void buildColumnarBatch(const SliceBatch& slice_batch,
                        const std::vector<std::string>& headers,
                        const ColumnarOptions& options,
                        ColumnarBatch& out);

/// Extract header row from first row of a SliceBatch (arena-backed).
std::vector<std::string> sliceRowToStrings(const SliceRow& row,
                                            const char* arena_data,
                                            std::size_t arena_size);

}  // namespace ultratab

#endif  // ULTRATAB_BATCH_BUILDER_H
