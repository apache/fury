extension MapEqualsExtension<K, V> on Map<K, V> {
  bool equals(Map<K, V>? other) {
    if (other == null) {
      return false;
    }
    if (identical(this, other)) {
      return true;
    }
    if (length != other.length) {
      return false;
    }
    for (final K key in keys) {
      if (!other.containsKey(key) || other[key] != this[key]) {
        return false;
      }
    }
    return true;
  }
}