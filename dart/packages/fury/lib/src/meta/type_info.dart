import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/meta/meta_string_byte.dart';
import 'package:fury/src/serializer/serializer.dart';

class TypeInfo {
  final Type dartType;
  final ObjType objType;
  final String? tag;
  final MetaStringBytes? typeNameBytes;
  final MetaStringBytes? nsBytes;
  late Serializer ser;

  TypeInfo(
    this.dartType,
    this.objType,
    this.tag,
    this.typeNameBytes,
    this.nsBytes,
  );

  TypeInfo.fromInnerType(
    this.dartType,
    this.objType,
    this.ser,
  ) : tag = null,
      typeNameBytes = null,
      nsBytes = null;
}