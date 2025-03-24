import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/datatype/fury_fixed_num.dart';
import 'package:fury/src/datatype/int16.dart';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer/primitive/primitive_serializer_cache.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';
import 'package:fury/src/serializer_pack.dart';

final class _Int16SerializerCache extends PrimitiveSerializerCache{
  static Int16Serializer? serRef;
  static Int16Serializer? serNoRef;

  const _Int16SerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Int16Serializer._(true);
      return serRef!;
    } else {
      serNoRef ??= Int16Serializer._(false);
      return serNoRef!;
    }
  }
}

// Dart does not have int16. Users can specify converting Dart's int to int16 via annotation, so an out-of-range error may occur
final class Int16Serializer extends Serializer<FixedNum>{

  static const SerializerCache cache = _Int16SerializerCache();

  Int16Serializer._(bool writeRef): super(ObjType.INT16, writeRef);

  @override
  Int16 read(ByteReader br, int refId, DeserializerPack pack) {
    return Int16(br.readInt16());
  }

  @override
  void write(ByteWriter bw, covariant Int16 v, SerPack pack) {
    // if (value < -32768 || value > 32767){
    //   throw FuryException.serRangeExcep(objType, value);
    // }
    bw.writeInt16(v.toInt());
  }
}
