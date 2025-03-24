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

  // void _quickSort<E>(int l, int r,
  //   int Function(int a, E ele) compare,
  //   void Function(int index1, int index2) swap,
  // ){
  //   if (l >= r) return;
  //   int i = l - 1;
  //   int j = r + 1;
  //   E pivot = this[(l + r) ~/ 2];
  //   while(i < j){
  //     do {
  //       ++i;
  //     } while(compare(i, pivot) < 0);
  //     do {
  //       --j;
  //     } while(compare(j, pivot) > 0);
  //     if (i < j) {
  //       // 交换元素
  //       var temp = this[i];
  //       this[i] = this[j];
  //       this[j] = temp;
  //       swap(i, j);
  //     }
  //   }
  //   _quickSort(l, j, compare, swap);
  //   _quickSort(j + 1, r, compare, swap);
  // }
  //
  // void sortByIndex<E>(
  //   int Function(int index1, E ele) compare,
  //   void Function(int index1, int index2) swap,
  // ) {
  //   if (isEmpty) return;
  //   int low = 0;
  //   int high = length - 1;
  //   _quickSort(low, high, compare, swap);
  // }
}