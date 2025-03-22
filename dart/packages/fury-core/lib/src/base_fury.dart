import 'dart:typed_data';

import 'meta/specs/custom_type_spec.dart';
import 'package:fury_core/src/memory/byte_writer.dart';
import 'package:fury_core/src/memory/byte_reader.dart';
import 'ser/ser.dart' show Ser;

abstract class BaseFury{
  void register(CustomTypeSpec spec, [String? tag]);
  void registerSerializer(Type type, Ser ser);
  // void registerAll(Map<String, FuryCustomTypeSpec> specs);
  Object? fromFury(Uint8List bytes, [ByteReader? br]);
  Uint8List toFury(Object? obj,);
  void toFuryWithWriter(Object? obj, ByteWriter writer);
}