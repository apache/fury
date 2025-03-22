import 'package:fury_core/src/dev_annotation/optimize.dart';

class MathChecker{

  @inline
  static bool validInt32(int value) {
    return value >= -2147483648 && value <= 2147483647;
  }
}