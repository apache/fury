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

// extension ListEqualsExtension<T> on List<T> {
//   /// Checks if this list is equal to another list.
//   bool isEqualTo(List<T>? other) {
//     // If other is null, they are not equal
//     if (other == null) {
//       return false;
//     }
//     // If lengths are different, they are not equal
//     if (length != other.length) {
//       return false;
//     }
//     // If they are the same object reference, they are equal
//     if (identical(this, other)) {
//       return true;
//     }
//     // Compare each element one by one
//     for (int index = 0; index < length; index += 1) {
//       if (this[index] != other[index]) {
//         return false;
//       }
//     }
//     return true;
//   }
// }

extension SetEquality<T> on Set<T> {
  /// Checks if this set is equal to another set.
  ///
  /// Two sets are considered equal if they contain the same elements,
  /// regardless of their order.
  ///
  /// Returns true if the sets are equal, false otherwise.
  /// If [other] is null, returns false (since this set is not null).
  bool equals(Set<T>? other) {
    if (other == null) {
      return false;
    }
    if (length != other.length) {
      return false;
    }
    if (identical(this, other)) {
      return true;
    }
    for (final T value in this) {
      if (!other.contains(value)) {
        return false;
      }
    }
    return true;
  }
}
