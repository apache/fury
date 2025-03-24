import 'package:collection/collection.dart';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/meta/specs/enum_spec.dart';

class TypeSpec{
  final Type type;
  final ObjType objType;
  final bool nullable;
  final bool certainForSer;
  final EnumSpec? enumSpec;
  final List<TypeSpec> genericsArgs;

  const TypeSpec(
    this.type,
    this.objType,
    this.nullable,
    this.certainForSer,
    this.enumSpec,
    this.genericsArgs,
  );

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
      (other is TypeSpec &&
        runtimeType == other.runtimeType &&
        type == other.type &&
        objType == other.objType &&
        nullable == other.nullable &&
        certainForSer == other.certainForSer &&
        enumSpec == other.enumSpec &&
        genericsArgs.equals(other.genericsArgs)
      );
  }
}