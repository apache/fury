import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/exception/deserialization_exception.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/meta/specs/enum_spec.dart';
import 'package:fury/src/serializer/custom_serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';
import 'package:fury/src/serializer_pack.dart';

final class _EnumSerializerCache extends SerializerCache{

  static final Map<Type, EnumSerializer> _cache = {};

  const _EnumSerializerCache();

  @override
  EnumSerializer getSerWithSpec(FuryConfig conf, covariant EnumSpec spec, Type dartType){
    EnumSerializer? ser = _cache[dartType];
    if (ser != null) {
      return ser;
    }
    // In furyJava, EnumSer does not perform reference tracking
    ser = EnumSerializer(false, spec.values);
    _cache[dartType] = ser;
    return ser;
  }
}

final class EnumSerializer extends CustomSerializer<Enum>{

  static const SerializerCache cache = _EnumSerializerCache();

  final List<Enum> values;
  EnumSerializer(bool writeRef, this.values): super(ObjType.NAMED_ENUM, writeRef);

  @override
  Enum read(ByteReader br, int refId, DeserializerPack pack) {
    int index = br.readVarUint32Small7();
    // furyJava supports deserializeNonexistentEnumValueAsNull,
    // but here in Dart, it will definitely throw an error if the index is out of range
    if (index < 0 || index >= values.length) {
      throw DeserializationRangeException(index, values);
    }
    return values[index];
  }

  @override
  void write(ByteWriter bw, Enum v, SerPack pack) {
    bw.writeVarUint32Small7(v.index);
  }
}
