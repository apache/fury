import 'dart:typed_data';
import 'meta/specs/custom_type_spec.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/serializer/serializer.dart' show Serializer;

abstract class BaseFury{
  void register(CustomTypeSpec spec, [String? tag]);
  void registerSerializer(Type type, Serializer ser);
  Object? fromFury(Uint8List bytes, [ByteReader? br]);
  Uint8List toFury(Object? obj,);
  void toFuryWithWriter(Object? obj, ByteWriter writer);
}