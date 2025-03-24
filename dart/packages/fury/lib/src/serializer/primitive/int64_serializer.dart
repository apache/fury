import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer/primitive/primitive_serializer_cache.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';
import 'package:fury/src/serializer_pack.dart';

final class _Int64SerializerCache extends PrimitiveSerializerCache{
  static Int64Serializer? serRef;
  static Int64Serializer? serNoRef;

  const _Int64SerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Int64Serializer._(true);
      return serRef!;
    } else {
      serNoRef ??= Int64Serializer._(false);
      return serNoRef!;
    }
  }
}

final class Int64Serializer extends Serializer<int> {

  static const SerializerCache cache = _Int64SerializerCache();

  Int64Serializer._(bool writeRef): super(ObjType.INT64, writeRef);

  @override
  int read(ByteReader br, int refId, DeserializerPack pack) {
    return br.readVarInt64();
  }

  @override
  void write(ByteWriter bw, int v, SerPack pack) {
    bw.writeVarInt64(v);
  }
}