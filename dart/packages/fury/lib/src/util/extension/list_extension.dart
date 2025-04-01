extension ListExtension on List {
  E? binarySearch<E>(E target, int Function(E a, E b) compare) {
    int low = 0;
    int high = length - 1;
    while (low <= high) {
      int mid = (low + high) >> 1;
      int cmp = compare(this[mid], target);
      if (cmp == 0) {
        return this[mid];
      } else if (cmp < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return null;
  }
}