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

#include "fury/meta/field_info.h"
#include "fury/meta/type_traits.h"
#include "fury/row/row.h"
#include "src/fury/row/writer.h"
#include <type_traits>
#include <utility>

namespace fury {

namespace encoder {

namespace details {

template <typename> struct ArrowSchemaBasicType;

template <> struct ArrowSchemaBasicType<bool> {
  static inline constexpr const auto value = arrow::boolean;
};

template <> struct ArrowSchemaBasicType<int8_t> {
  static inline constexpr const auto value = arrow::int8;
};

template <> struct ArrowSchemaBasicType<int16_t> {
  static inline constexpr const auto value = arrow::int16;
};

template <> struct ArrowSchemaBasicType<int32_t> {
  static inline constexpr const auto value = arrow::int32;
};

template <> struct ArrowSchemaBasicType<int64_t> {
  static inline constexpr const auto value = arrow::int64;
};

template <> struct ArrowSchemaBasicType<float> {
  static inline constexpr const auto value = arrow::float32;
};

template <> struct ArrowSchemaBasicType<double> {
  static inline constexpr const auto value = arrow::float64;
};

inline std::string StringViewToString(std::string_view s) {
  return {s.begin(), s.end()};
}

template <auto> using Void = void;

} // namespace details

using meta::FuryFieldInfo;

// RowEncodeTrait<T> defines how to serialize `T` to the row format
// it includes:
// - Type(...): construct arrow format type of type `T`
// - Schema(...): construct schema of type `T` (only for class types)
// - Write(const T&, ...): encode `T` via the provided writer
template <typename T, typename Enable = void> struct RowEncodeTrait;

template <typename T>
struct RowEncodeTrait<T,
                      details::Void<details::ArrowSchemaBasicType<T>::value>> {

  static auto Type() { return details::ArrowSchemaBasicType<T>::value(); }

  static auto Write(const T &value, RowWriter &writer, int index) {
    return writer.Write(index, value);
  }
};

template <typename T>
struct RowEncodeTrait<T, std::enable_if_t<std::is_class_v<T>>> {
  template <typename FieldInfo, size_t... I>
  static arrow::FieldVector FieldVectorImpl(std::index_sequence<I...>) {
    return {arrow::field(
        details::StringViewToString(FieldInfo::Names[I]),
        RowEncodeTrait<meta::RemoveMemberPointerCVRefT<decltype(std::get<I>(
            FieldInfo::Ptrs))>>::Type())...};
  }

  static auto FieldVector() {
    using FieldInfo = decltype(FuryFieldInfo(std::declval<T>()));

    return FieldVectorImpl<FieldInfo>(
        std::make_index_sequence<FieldInfo::Size>());
  }

  static auto Type() { return arrow::struct_(FieldVector()); }

  static auto Schema() { return arrow::schema(FieldVector()); }

  template <typename FieldInfo, size_t... I>
  static auto WriteImpl(const T &value, RowWriter &writer,
                        std::index_sequence<I...>) {
    (RowEncodeTrait<meta::RemoveMemberPointerCVRefT<decltype(std::get<I>(
         FieldInfo::Ptrs))>>::Write(value.*std::get<I>(FieldInfo::Ptrs), writer,
                                    I),
     ...);
  }

  static auto Write(const T &value, RowWriter &writer) {
    using FieldInfo = decltype(FuryFieldInfo(std::declval<T>()));

    return WriteImpl<FieldInfo>(value, writer,
                                std::make_index_sequence<FieldInfo::Size>());
  }
};

} // namespace encoder

} // namespace fury
