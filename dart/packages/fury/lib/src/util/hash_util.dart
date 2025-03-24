class HashUtil {
  static int hashIntList(List<int> list){
    int res = 1;
    for (var i in list) {
      res = 31 * res + i;
    }
    return res;
  }
}