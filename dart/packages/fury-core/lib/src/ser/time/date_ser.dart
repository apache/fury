import 'package:fury_core/src/ser/ser.dart' show Ser;
import 'package:fury_core/src/ser/time/time_ser_cache.dart';
import 'package:fury_core/src/util/math_checker.dart';

import '../../const/obj_type.dart';
import '../../fury_data_type/local_date.dart';
import '../../memory/byte_reader.dart';
import '../../memory/byte_writer.dart';
import '../../deser_pack.dart';
import '../../ser_pack.dart';
import '../ser_cache.dart';


final class _DateSerCache extends TimeSerCache{
  static DateSer? serRef;
  static DateSer? serNoRef;

  const _DateSerCache();

  @override
  Ser getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= DateSer._(true);
      return serRef!;
    } else {
      serNoRef ??= DateSer._(false);
      return serNoRef!;
    }
  }
}


final class DateSer extends Ser<LocalDate> {

  static const SerCache cache = _DateSerCache();

  DateSer._(bool writeRef) : super(ObjType.LOCAL_DATE, writeRef);

  @override
  LocalDate read(ByteReader br, int refId, DeserPack pack) {
    return LocalDate.fromEpochDay(br.readInt32(), utc: true);
  }

  @override
  void write(ByteWriter bw, LocalDate v, SerPack pack) {
    int days = v.toEpochDay(utc: true);
    if (!MathChecker.validInt32(days)){
      throw ArgumentError('Date toEpochDay is not valid int32: $days');
    }
    bw.writeInt32(days);
  }
}