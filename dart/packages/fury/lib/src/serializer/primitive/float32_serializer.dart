import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/datatype/float32.dart';
import 'package:fury/src/datatype/fury_fixed_num.dart';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer/primitive/primitive_serializer_cache.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';
import 'package:fury/src/serializer_pack.dart';

final class _Float32SerializerCache extends PrimitiveSerializerCache{
  static Float32Serializer? serRef;
  static Float32Serializer? serNoRef;

  const _Float32SerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Float32Serializer._(true);
      return serRef!;
    } else {
      serNoRef ??= Float32Serializer._(false);
      return serNoRef!;
    }
  }
}

// Dart does not have float32; the user can specify converting Dart double to float32 through annotation, so precision errors may occur
final class Float32Serializer extends Serializer<FixedNum>{

  static const SerializerCache cache = _Float32SerializerCache();

  Float32Serializer._(bool writeRef): super(ObjType.FLOAT32, writeRef);

  @override
  Float32 read(ByteReader br, int refId, DeserializerPack pack) {
    return Float32(br.readFloat32());
  }

  @override
  void write(ByteWriter bw, covariant Float32 v, SerPack pack) {
    // No checks are performed here
    // if (value.isInfinite || value.isNaN || value < -3.4028235e38 || value > 3.4028235e38){
    //   throw FuryException.serRangeExcep(objType, value);
    // }
    bw.writeFloat32(v.toDouble());
  }
}
