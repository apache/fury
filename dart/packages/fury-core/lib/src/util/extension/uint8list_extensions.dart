import 'dart:typed_data';

extension Uint8ListExtensions on Uint8List {
  /// Compares two [Uint8List]s by comparing 8 bytes at a time.
  bool memEquals(Uint8List other) {
    if (identical(this, other)) {
      return true;
    }
    if (lengthInBytes != other.lengthInBytes) {
      return false;
    }
    // Treat the original byte lists as lists of 8-byte words.
    var numWords = lengthInBytes ~/ 8;
    var words1 = buffer.asUint64List(0, numWords);
    var words2 = other.buffer.asUint64List(0, numWords);

    for (var i = 0; i < words1.length; i += 1) {
      if (words1[i] != words2[i]) {
        return false;
      }
    }
    // Compare any remaining bytes.
    for (var i = words1.lengthInBytes; i < lengthInBytes; i += 1) {
      if (this[i] != other[i]) {
        return false;
      }
    }
    return true;
  }
}