import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer_pack.dart';

/// Planned to only handle non-null type serializers. Null values should be handled externally.
abstract base class Serializer<T> {

  final ObjType objType;
  // final bool forceNoRefWrite; // Not controlled by Fury's Config; indicates no reference writing for this type
  final bool writeRef; // Indicates whether to write references

  const Serializer(
    this.objType,
    this.writeRef,
    // [this.forceNoRefWrite = false]
  );
  T read(ByteReader br, int refId, DeserializerPack pack);

  void write(ByteWriter bw, T v, SerPack pack);

  String get tag => throw UnimplementedError('tag is not implemented');
}
