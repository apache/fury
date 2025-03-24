import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/datatype/fury_fixed_num.dart';
import 'package:fury/src/datatype/int8.dart';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer/primitive/primitive_serializer_cache.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';
import 'package:fury/src/serializer_pack.dart';

final class _Int8SerializerCache extends PrimitiveSerializerCache{
  static Int8Serializer? serRef;
  static Int8Serializer? serNoRef;

  const _Int8SerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Int8Serializer._(true);
      return serRef!;
    } else {
      serNoRef ??= Int8Serializer._(false);
      return serNoRef!;
    }
  }
}

// Dart does not have an int8 type. Users can specify converting a Dart int to int8 via annotations, so out-of-range errors may occur
final class Int8Serializer extends Serializer<FixedNum>{

  static const SerializerCache cache = _Int8SerializerCache();

  Int8Serializer._(bool writeRef): super(ObjType.INT8, writeRef);

  @override
  Int8 read(ByteReader br, int refId, DeserializerPack pack) {
    return Int8(br.readInt8());// Use signed 8-bit integer, which is consistent with byte in FuryJava
  }

  @override
  void write(ByteWriter bw, covariant Int8 v, SerPack pack) {
    // if (value < -128 || value > 127){
    //   throw FuryException.serRangeExcep(objType, value);
    // }
    bw.writeInt8(v.toInt());
  }
}
