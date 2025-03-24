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
