/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "fury/encoder/row_encode_trait.h"
#include <memory>
#include <type_traits>

namespace fury {

namespace encoder {

template <typename T> struct RowEncoder {
  static_assert(std::is_class_v<T>, "currently only class types are supported");

  RowEncoder()
      : writer_(std::make_unique<RowWriter>(RowEncodeTrait<T>::Schema())) {
    writer_->Reset();
  }

  void Encode(const T &value) {
    RowEncodeTrait<T>::Write(DefaultWriteVisitor{children_}, value,
                             GetWriter());
  }

  RowWriter &GetWriter() const { return *writer_.get(); }
  const std::vector<std::unique_ptr<RowWriter>> &GetChildren() const {
    return children_;
  }
  const arrow::Schema &GetSchema() const { return *writer_->schema().get(); }

  void ResetChildren() { children_.clear(); }

private:
  std::unique_ptr<RowWriter> writer_;
  std::vector<std::unique_ptr<RowWriter>> children_;
};

} // namespace encoder

} // namespace fury
