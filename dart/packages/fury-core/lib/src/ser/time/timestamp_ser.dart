import 'package:fury_core/src/ser/ser.dart' show Ser;
import 'package:fury_core/src/ser/time/time_ser_cache.dart';

import '../../const/obj_type.dart';
import '../../fury_data_type/timestamp.dart';
import '../../memory/byte_reader.dart';
import '../../memory/byte_writer.dart';
import '../../deser_pack.dart';
import '../../ser_pack.dart';
import '../ser_cache.dart';


final class _TimestampSerCache extends TimeSerCache{
  static TimestampSer? serRef;
  static TimestampSer? serNoRef;

  const _TimestampSerCache();

  @override
  Ser getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= TimestampSer._(true);
      return serRef!;
    } else {
      serNoRef ??= TimestampSer._(false);
      return serNoRef!;
    }
  }
}

/// 不明白fury Java为何不把TimeStamp注册到defaultSerializer中,而是每次都要写xtypeId
final class TimestampSer extends Ser<TimeStamp> {

  static const SerCache cache = _TimestampSerCache();

  TimestampSer._(bool writeRef) : super(ObjType.TIMESTAMP, writeRef);

  @override
  TimeStamp read(ByteReader br, int refId, DeserPack pack) {
    int microseconds = br.readInt64();
    // 这里需要注意，UTC时间戳
    return TimeStamp(microseconds);
  }

  @override
  void write(ByteWriter bw, TimeStamp v, SerPack pack) {
    bw.writeInt64(v.microsecondsSinceEpoch);
  }
}