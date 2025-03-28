import 'package:fury/src/dev_annotation/optimize.dart';

final class CharUtil{
  @inline
  static bool isLUD(int c){ // Lower or Upper or Digit
    return (c >= 0x30 && c <= 0x39) || // 0-9
        (c >= 0x41 && c <= 0x5A) || // A-Z
        (c >= 0x61 && c <= 0x7A);   // a-z
  }

  @inline
  static bool isLS(int c){ // Lower or Special
    return (c >= 0x61 && c <= 0x7A) || // a-z
        (c == 0x24) || // $
        (c == 0x5F) || // _
        (c == 0x2E) || // .
        (c == 0x7C);   // |
  }

  @inline
  static bool digit(int c){ // Digit
    return (c >= 0x30 && c <= 0x39); // 0-9
  }

  @inline
  static bool upper(int c){ // Upper
    return (c >= 0x41 && c <= 0x5A); // A-Z
  }

  @inline
  static int toLowerChar(int charCode) {
    return charCode + 32;
  }
}