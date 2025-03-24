import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/datatype/fury_fixed_num.dart';
import 'package:fury/src/datatype/int32.dart';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer/primitive/primitive_serializer_cache.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';
import 'package:fury/src/serializer_pack.dart';

final class _Int32SerializerCache extends PrimitiveSerializerCache{
  static Int32Serializer? serRef;
  static Int32Serializer? serNoRef;

  const _Int32SerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Int32Serializer._(true);
      return serRef!;
    } else {
      serNoRef ??= Int32Serializer._(false);
      return serNoRef!;
    }
  }
}

// Dart does not have int32, users specify converting dart int to int32 through annotations, so range errors may occur
final class Int32Serializer extends Serializer<FixedNum>{
  static const SerializerCache cache = _Int32SerializerCache();

  Int32Serializer._(bool writeRef): super(ObjType.INT32, writeRef);

  @override
  Int32 read(ByteReader br, int refId, DeserializerPack pack) {
    int res = br.readVarInt32();
    return Int32(res);
  }

  @override
  void write(ByteWriter bw, covariant Int32 v, SerPack pack) {
    // No check is done here directly
    // if (value < -2147483648 || value > 2147483647){
    //   throw FuryException.serRangeExcep(objType, value);
    // }
    bw.writeVarInt32(v.toInt());
  }
}
