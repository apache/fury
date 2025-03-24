extension IntExtensions on int {
  int rotateLeft(int n) {
    return (this << n) | (this >>> (64 - n));
  }
}