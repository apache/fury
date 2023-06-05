#pragma once

#include "arrow/api.h"
#include <memory>

namespace fury {

std::shared_ptr<arrow::ListType>
list(const std::shared_ptr<arrow::DataType> &type);

std::shared_ptr<arrow::MapType>
map(const std::shared_ptr<arrow::DataType> &key_type,
    const std::shared_ptr<arrow::DataType> &value_type,
    bool keys_sorted = false);

int64_t get_byte_width(const std::shared_ptr<arrow::DataType> &dtype);

} // namespace fury
