import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer/primitive/primitive_serializer_cache.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';
import 'package:fury/src/serializer_pack.dart';

final class _Float64SerializerCache extends PrimitiveSerializerCache{
  static Float64Serializer? serRef;
  static Float64Serializer? serNoRef;

  const _Float64SerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Float64Serializer._(true);
      return serRef!;
    } else {
      serNoRef ??= Float64Serializer._(false);
      return serNoRef!;
    }
  }
}

final class Float64Serializer extends Serializer<double>{

  static const SerializerCache cache = _Float64SerializerCache();

  Float64Serializer._(bool writeRef): super(ObjType.FLOAT64, writeRef);

  @override
  double read(ByteReader br, int refId, DeserializerPack pack) {
    return br.readFloat64();
  }

  @override
  void write(ByteWriter bw, double v, SerPack pack) {
    bw.writeFloat64(v);
  }
}