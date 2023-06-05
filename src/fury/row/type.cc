#include "fury/row/type.h"

namespace fury {

std::shared_ptr<arrow::ListType>
list(const std::shared_ptr<arrow::DataType> &type) {
  auto t = arrow::list(type);
  return std::dynamic_pointer_cast<arrow::ListType>(t);
}

std::shared_ptr<arrow::MapType>
map(const std::shared_ptr<arrow::DataType> &key_type,
    const std::shared_ptr<arrow::DataType> &value_type, bool keys_sorted) {
  auto t = arrow::map(key_type, value_type, keys_sorted);
  return std::dynamic_pointer_cast<arrow::MapType>(t);
}

int64_t get_byte_width(const std::shared_ptr<arrow::DataType> &dtype) {
  if (auto ptr = dynamic_cast<arrow::FixedWidthType *>(dtype.get())) {
    return ptr->bit_width() / CHAR_BIT;
  } else {
    return -1;
  }
}

} // namespace fury
