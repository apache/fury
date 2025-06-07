/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "fory/row/type.h"

namespace fory {

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

} // namespace fory
