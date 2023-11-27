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

#include <type_traits>

namespace fury {

namespace meta {

// dependent name for constant `false` to workaround static_assert(false) issue
// before C++23
template <typename T> constexpr inline bool AlwaysFalse = false;

// T U::* -> T
template <typename> struct RemoveMemberPointer;

template <typename T, typename U> struct RemoveMemberPointer<T U::*> {
  using type = T;
};

template <typename T>
using RemoveMemberPointerT = typename RemoveMemberPointer<T>::type;

// same as std::remove_cvref_t since C++20
template <typename T>
using RemoveCVRefT = std::remove_cv_t<std::remove_reference_t<T>>;

template <typename T>
using RemoveMemberPointerCVRefT = RemoveMemberPointerT<RemoveCVRefT<T>>;

template <auto V1, auto V2>
inline constexpr bool IsSameValue =
    std::is_same_v<std::integral_constant<decltype(V1), V1>,
                   std::integral_constant<decltype(V2), V2>>;

template <auto V, auto... Vs>
inline constexpr bool ContainsValue =
    std::disjunction_v<std::bool_constant<IsSameValue<V, Vs>>...>;

template <auto...> struct IsUnique : std::true_type {};

template <auto V1, auto... VN>
struct IsUnique<V1, VN...>
    : std::bool_constant<!ContainsValue<V1, VN...> && IsUnique<VN...>::value> {
};

} // namespace meta

} // namespace fury
