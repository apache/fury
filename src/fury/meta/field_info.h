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

#include "fury/meta/preprocessor.h"
#include <array>
#include <string_view>
#include <tuple>
#include <type_traits>

namespace fury {

namespace meta {

namespace details {

// dependent name for constant `false` to workaround static_assert(false) issue
// before C++23
template <typename T> constexpr inline bool AlwaysFalse = false;

} // namespace details

// decltype(FuryFieldInfo<T>(v)) records field meta information for type T
// it includes:
// - number of fields: typed size_t
// - field names: typed `std::string_view`
// - field member points: typed `decltype(a) T::*` for any member `T::a`
template <typename T> constexpr auto FuryFieldInfo(const T &) noexcept {
  static_assert(details::AlwaysFalse<T>,
                "FURY_FIELD_INFO for type T is expected but not defined");
}

} // namespace meta

} // namespace fury

#define FURY_FIELD_INFO_NAMES_FUNC(field) #field,
#define FURY_FIELD_INFO_PTRS_FUNC(type, field) &type::field,

// here we define function overloads in the current namespace rather than
// template specialization of classes since specialization of template in
// different namespace is hard
// NOTE: for performing ADL (argument-dependent lookup),
// `FURY_FIELD_INFO(T, ...)` must be defined in the same namespace as `T`
#define FURY_FIELD_INFO(type, ...)                                             \
  static_assert(std::is_class_v<type>, "it must be a class type");             \
  template <typename> struct FuryFieldInfoImpl;                                \
  template <> struct FuryFieldInfoImpl<type> {                                 \
    static inline constexpr size_t Size = FURY_PP_NARG(__VA_ARGS__);           \
    static inline constexpr std::string_view Name = #type;                     \
    static inline constexpr std::array<std::string_view, Size> Names = {       \
        FURY_PP_FOREACH(FURY_FIELD_INFO_NAMES_FUNC, __VA_ARGS__)};             \
    static inline constexpr auto Ptrs = std::tuple{                            \
        FURY_PP_FOREACH_1(FURY_FIELD_INFO_PTRS_FUNC, type, __VA_ARGS__)};      \
  };                                                                           \
  inline constexpr auto FuryFieldInfo(const type &) noexcept {                 \
    return FuryFieldInfoImpl<type>{};                                          \
  };
