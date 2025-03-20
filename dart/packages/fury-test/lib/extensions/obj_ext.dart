extension ObjExt on Object {
  bool strEquals(Object other) {
    if (identical(this, other)) {
      return true;
    }
    return toString() == other.toString();
  }
}