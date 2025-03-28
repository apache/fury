import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/datatype/local_date.dart';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';
import 'package:fury/src/serializer/time/time_serializer_cache.dart';
import 'package:fury/src/serializer_pack.dart';
import 'package:fury/src/util/math_checker.dart';

final class _DateSerializerCache extends TimeSerializerCache{
  static DateSerializer? serRef;
  static DateSerializer? serNoRef;

  const _DateSerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= DateSerializer._(true);
      return serRef!;
    } else {
      serNoRef ??= DateSerializer._(false);
      return serNoRef!;
    }
  }
}


final class DateSerializer extends Serializer<LocalDate> {

  static const SerializerCache cache = _DateSerializerCache();

  DateSerializer._(bool writeRef) : super(ObjType.LOCAL_DATE, writeRef);

  @override
  LocalDate read(ByteReader br, int refId, DeserializerPack pack) {
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