// extension ListEqualsExtension<T> on List<T> {
//   /// 检查此列表是否与另一列表内容相等
//   bool isEqualTo(List<T>? other) {
//     // 如果other为null，则不相等
//     if (other == null) {
//       return false;
//     }
//     // 如果长度不同，则不相等
//     if (length != other.length) {
//       return false;
//     }
//     // 如果是同一个对象引用，则相等
//     if (identical(this, other)) {
//       return true;
//     }
//     // 逐个元素比较
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