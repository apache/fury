import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer/primitive/primitive_serializer_cache.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';
import 'package:fury/src/serializer_pack.dart';

final class _BoolSerializerCache extends PrimitiveSerializerCache{
  static BoolSerializer? serRef;
  static BoolSerializer? serNoRef;

  const _BoolSerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= BoolSerializer._(true);
      return serRef!;
    } else {
      serNoRef ??= BoolSerializer._(false);
      return serNoRef!;
    }
  }
}


final class BoolSerializer extends Serializer<bool> {

  static const SerializerCache cache = _BoolSerializerCache();

  BoolSerializer._(bool writeRef): super(ObjType.BOOL, writeRef);

  @override
  bool read(ByteReader br, int refId, DeserializerPack pack) {
    return br.readUint8() != 0;
  }

  @override
  void write(ByteWriter bw, bool v, SerPack pack) {
    bw.writeBool(v);
  }
}