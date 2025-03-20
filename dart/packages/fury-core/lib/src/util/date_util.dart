import 'package:fury_core/src/dev_annotation/optimize.dart';

class DateUtil{
  static final DateTime epoch = DateTime(1970, 1, 1);

  @inline
  static int toEpochDay(DateTime date) {
    return date.difference(epoch).inDays;
  }

  @inline
  static DateTime fromEpochDay(int days) {
    return epoch.add(Duration(days: days));
  }
}