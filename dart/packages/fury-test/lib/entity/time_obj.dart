import 'package:fury/fury.dart';

part '../generated/time_obj.g.dart';

@FuryClass(promiseAcyclic: true)
class TimeObj with _$TimeObjFury{
  final LocalDate date1;
  final LocalDate date2;
  final LocalDate date3;
  final LocalDate date4;
  final TimeStamp dateTime1;
  final TimeStamp dateTime2;
  final TimeStamp dateTime3;
  final TimeStamp dateTime4;

  const TimeObj(
    this.date1,
    this.date2,
    this.date3,
    this.date4,
    this.dateTime1,
    this.dateTime2,
    this.dateTime3,
    this.dateTime4,
  );

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        (other is TimeObj &&
            runtimeType == other.runtimeType &&
            date1 == other.date1 &&
            date2 == other.date2 &&
            date3 == other.date3 &&
            date4 == other.date4 &&
            dateTime1 == other.dateTime1 &&
            dateTime2 == other.dateTime2 &&
            dateTime3 == other.dateTime3 &&
            dateTime4 == other.dateTime4);
  }
}