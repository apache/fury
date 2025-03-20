import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/meta/meta_string_byte.dart';
import 'package:fury_core/src/ser/ser.dart' show Ser;

class ClassInfo {
  final Type dartType;
  final ObjType objType;
  final String? tag;
  final MetaStringBytes? typeNameBytes;
  final MetaStringBytes? nsBytes;
  late Ser ser;

  ClassInfo(
    this.dartType,
    this.objType,
    this.tag,
    this.typeNameBytes,
    this.nsBytes,
  );

  ClassInfo.fromInnerType(
    this.dartType,
    this.objType,
    this.ser,
  ) : tag = null,
      typeNameBytes = null,
      nsBytes = null;
}