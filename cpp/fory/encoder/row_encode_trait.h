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

#pragma once

#include "fory/meta/field_info.h"
#include "fory/meta/type_traits.h"
#include "fory/row/writer.h"
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>

namespace fory {

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

template <typename T> inline constexpr bool IsMap = meta::IsPairIterable<T>;

template <typename T>
inline constexpr bool IsArray =
    meta::IsIterable<T> && !IsString<T> && !IsMap<T>;

template <typename> inline constexpr bool IsOptional = false;

template <typename T> inline constexpr bool IsOptional<std::optional<T>> = true;

template <typename T>
inline constexpr bool IsClassButNotBuiltin =
    std::is_class_v<T> &&
    !(IsString<T> || IsArray<T> || IsOptional<T> || IsMap<T>);

inline decltype(auto) GetChildType(RowWriter &writer, int index) {
  return writer.schema()->field(index)->type();
}

inline decltype(auto) GetChildType(ArrayWriter &writer, int index) {
  return writer.type()->field(0)->type();
}

} // namespace details

using meta::ForyFieldInfo;

struct EmptyWriteVisitor {
  template <typename, typename T> void Visit(T &&) {}
};

template <typename C> struct DefaultWriteVisitor {
  C &cont;

  DefaultWriteVisitor(C &cont) : cont(cont) {}

  template <typename, typename T> void Visit(std::unique_ptr<T> writer) {
    cont.push_back(std::move(writer));
  }
};

// RowEncodeTrait<T> defines how to serialize `T` to the row format
// it includes:
// - Type(): construct arrow format type of type `T`
// - Schema(): construct schema of type `T` (only for class types)
// - Write(auto&& visitor, const T& value, ...):
//     encode `T` via the provided writer
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

  template <typename V, typename W,
            std::enable_if_t<meta::IsOneOf<W, RowWriter, ArrayWriter>::value,
                             int> = 0>
  static void Write(V &&, const T &value, W &writer, int index) {
    writer.Write(index, value);
  }
};

template <typename T>
struct RowEncodeTrait<
    T, std::enable_if_t<details::IsString<std::remove_cv_t<T>>>> {
  static auto Type() { return arrow::utf8(); }

  template <typename V, typename W,
            std::enable_if_t<meta::IsOneOf<W, RowWriter, ArrayWriter>::value,
                             int> = 0>
  static void Write(V &&, const T &value, W &writer, int index) {
    writer.WriteString(index, value);
  }
};

template <typename T>
struct RowEncodeTrait<
    T, std::enable_if_t<details::IsOptional<std::remove_cv_t<T>>>> {
  static auto Type() { return RowEncodeTrait<typename T::value_type>::Type(); }

  template <typename V, typename W,
            std::enable_if_t<meta::IsOneOf<W, RowWriter, ArrayWriter>::value,
                             int> = 0>
  static void Write(V &&visitor, const T &value, W &writer, int index) {
    if (value) {
      RowEncodeTrait<typename T::value_type>::Write(std::forward<V>(visitor),
                                                    *value, writer, index);
    } else {
      writer.SetNullAt(index);
    }
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

  template <typename FieldInfo, typename V, size_t... I>
  static void WriteImpl(V &&visitor, const T &value, RowWriter &writer,
                        std::index_sequence<I...>) {
    (RowEncodeTrait<meta::RemoveMemberPointerCVRefT<decltype(std::get<I>(
         FieldInfo::Ptrs))>>::Write(std::forward<V>(visitor),
                                    value.*std::get<I>(FieldInfo::Ptrs), writer,
                                    I),
     ...);
  }

public:
  static auto FieldVector() {
    using FieldInfo = decltype(ForyFieldInfo(std::declval<T>()));

    return FieldVectorImpl<FieldInfo>(
        std::make_index_sequence<FieldInfo::Size>());
  }

  static auto Type() { return arrow::struct_(FieldVector()); }

  static auto Schema() { return arrow::schema(FieldVector()); }

  template <typename V>
  static auto Write(V &&visitor, const T &value, RowWriter &writer) {
    using FieldInfo = decltype(ForyFieldInfo(std::declval<T>()));

    return WriteImpl<FieldInfo>(std::forward<V>(visitor), value, writer,
                                std::make_index_sequence<FieldInfo::Size>());
  }

  template <typename V, typename W,
            std::enable_if_t<meta::IsOneOf<W, RowWriter, ArrayWriter>::value,
                             int> = 0>
  static void Write(V &&visitor, const T &value, W &writer, int index) {
    auto offset = writer.cursor();

    auto inner_writer = std::make_unique<RowWriter>(
        arrow::schema(details::GetChildType(writer, index)->fields()), &writer);

    inner_writer->Reset();
    RowEncodeTrait<T>::Write(std::forward<V>(visitor), value,
                             *inner_writer.get());

    writer.SetOffsetAndSize(index, offset, writer.cursor() - offset);

    std::forward<V>(visitor).template Visit<std::remove_cv_t<T>>(
        std::move(inner_writer));
  }
};

template <typename T>
struct RowEncodeTrait<T,
                      std::enable_if_t<details::IsArray<std::remove_cv_t<T>>>> {
  static auto Type() {
    return arrow::list(RowEncodeTrait<meta::GetValueType<T>>::Type());
  }

  template <typename V>
  static void Write(V &&visitor, const T &value, ArrayWriter &writer) {
    int index = 0;
    for (const auto &v : value) {
      RowEncodeTrait<meta::GetValueType<T>>::Write(std::forward<V>(visitor), v,
                                                   writer, index);
      ++index;
    }
  }

  template <typename V, typename W,
            std::enable_if_t<meta::IsOneOf<W, RowWriter, ArrayWriter>::value,
                             int> = 0>
  static void Write(V &&visitor, const T &value, W &writer, int index) {
    auto offset = writer.cursor();

    auto inner_writer = std::make_unique<ArrayWriter>(
        std::dynamic_pointer_cast<arrow::ListType>(
            details::GetChildType(writer, index)),
        &writer);

    inner_writer->Reset(value.size());
    RowEncodeTrait<T>::Write(std::forward<V>(visitor), value,
                             *inner_writer.get());

    writer.SetOffsetAndSize(index, offset, writer.cursor() - offset);

    std::forward<V>(visitor).template Visit<std::remove_cv_t<T>>(
        std::move(inner_writer));
  }
};

template <typename T>
struct RowEncodeTrait<T,
                      std::enable_if_t<details::IsMap<std::remove_cv_t<T>>>> {
  static auto Type() {
    return arrow::map(
        RowEncodeTrait<typename T::value_type::first_type>::Type(),
        RowEncodeTrait<typename T::value_type::second_type>::Type());
  }

  template <typename V>
  static void WriteKey(V &&visitor, const T &value, ArrayWriter &writer) {
    int index = 0;
    for (const auto &v : value) {
      RowEncodeTrait<typename T::value_type::first_type>::Write(
          std::forward<V>(visitor), v.first, writer, index);
      ++index;
    }
  }

  template <typename V>
  static void WriteValue(V &&visitor, const T &value, ArrayWriter &writer) {
    int index = 0;
    for (const auto &v : value) {
      RowEncodeTrait<typename T::value_type::second_type>::Write(
          std::forward<V>(visitor), v.second, writer, index);
      ++index;
    }
  }

  template <typename V, typename W,
            std::enable_if_t<meta::IsOneOf<W, RowWriter, ArrayWriter>::value,
                             int> = 0>
  static void Write(V &&visitor, const T &value, W &writer, int index) {
    auto offset = writer.cursor();
    writer.WriteDirectly(-1);

    auto map_type = std::dynamic_pointer_cast<arrow::MapType>(
        details::GetChildType(writer, index));

    auto key_writer =
        std::make_unique<ArrayWriter>(std::static_pointer_cast<arrow::ListType>(
                                          arrow::list(map_type->key_type())),
                                      &writer);

    key_writer->Reset(value.size());
    RowEncodeTrait<T>::WriteKey(std::forward<V>(visitor), value,
                                *key_writer.get());

    writer.WriteDirectly(offset, key_writer->size());

    auto value_writer =
        std::make_unique<ArrayWriter>(std::static_pointer_cast<arrow::ListType>(
                                          arrow::list(map_type->item_type())),
                                      &writer);

    value_writer->Reset(value.size());
    RowEncodeTrait<T>::WriteValue(std::forward<V>(visitor), value,
                                  *value_writer.get());

    writer.SetOffsetAndSize(index, offset, writer.cursor() - offset);

    std::forward<V>(visitor).template Visit<std::remove_cv_t<T>>(
        std::move(key_writer));
    std::forward<V>(visitor).template Visit<std::remove_cv_t<T>>(
        std::move(value_writer));
  }
};

} // namespace encoder

} // namespace fory
