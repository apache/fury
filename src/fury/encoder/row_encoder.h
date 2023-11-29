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
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

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

template <typename T>
inline constexpr bool IsString =
    meta::IsOneOf<T, std::string, std::string_view>::value;

template <typename T>
inline constexpr bool IsClassButNotBuiltin = std::is_class_v<T> && !IsString<T>;

} // namespace details

using meta::FuryFieldInfo;

// RowEncodeTrait<T> defines how to serialize `T` to the row format
// it includes:
// - Type(...): construct arrow format type of type `T`
// - Schema(...): construct schema of type `T` (only for class types)
// - Write(const T&, ...): encode `T` via the provided writer
template <typename T, typename Enable = void> struct RowEncodeTrait {
  static_assert(meta::AlwaysFalse<T>,
                "type T is currently not supported for encoding");
};

template <typename T>
struct RowEncodeTrait<
    T, meta::Void<details::ArrowSchemaBasicType<std::remove_cv_t<T>>::value>> {

  static auto Type() {
    return details::ArrowSchemaBasicType<std::remove_cv_t<T>>::value();
  }

  static auto Write(const T &value, RowWriter &writer, int index) {
    writer.Write(index, value);
    return std::monostate{};
  }
};

template <typename T>
struct RowEncodeTrait<
    T, std::enable_if_t<details::IsString<std::remove_cv_t<T>>>> {
  static auto Type() { return arrow::utf8(); }

  static auto Write(const T &value, RowWriter &writer, int index) {
    writer.WriteString(index, value);
    return std::monostate{};
  }
};

template <typename T>
struct RowEncodeTrait<
    T, std::enable_if_t<details::IsClassButNotBuiltin<std::remove_cv_t<T>>>> {
private:
  template <typename FieldInfo, size_t... I>
  static arrow::FieldVector FieldVectorImpl(std::index_sequence<I...>) {
    return {arrow::field(
        details::StringViewToString(FieldInfo::Names[I]),
        RowEncodeTrait<meta::RemoveMemberPointerCVRefT<decltype(std::get<I>(
            FieldInfo::Ptrs))>>::Type())...};
  }

  template <typename FieldInfo, size_t... I>
  static auto WriteImpl(const T &value, RowWriter &writer,
                        std::index_sequence<I...>) {
    return std::tuple{
        RowEncodeTrait<meta::RemoveMemberPointerCVRefT<decltype(std::get<I>(
            FieldInfo::Ptrs))>>::Write(value.*std::get<I>(FieldInfo::Ptrs),
                                       writer, I)...};
  }

public:
  static auto FieldVector() {
    using FieldInfo = decltype(FuryFieldInfo(std::declval<T>()));

    return FieldVectorImpl<FieldInfo>(
        std::make_index_sequence<FieldInfo::Size>());
  }

  static auto Type() { return arrow::struct_(FieldVector()); }

  static auto Schema() { return arrow::schema(FieldVector()); }

  static auto Write(const T &value, RowWriter &writer) {
    using FieldInfo = decltype(FuryFieldInfo(std::declval<T>()));

    return WriteImpl<FieldInfo>(value, writer,
                                std::make_index_sequence<FieldInfo::Size>());
  }

  static auto Write(const T &value, RowWriter &writer, int index) {
    auto offset = writer.cursor();

    auto inner_writer = std::make_shared<RowWriter>(
        arrow::schema(writer.schema()->field(index)->type()->fields()),
        &writer);

    inner_writer->Reset();
    RowEncodeTrait<std::remove_cv_t<T>>::Write(value, *inner_writer.get());

    writer.SetOffsetAndSize(index, offset, writer.cursor() - offset);
    return inner_writer;
  }
};

} // namespace encoder

} // namespace fury
