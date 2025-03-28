import 'package:meta/meta.dart';
import 'package:fury/src/const/obj_type.dart';

@immutable
class TypeImmutable {
  final int typeLibId;
  final String name;
  final ObjType objType;
  final bool independent;
  final bool certainForSer;

  TypeImmutable(
    this.name,
    this.typeLibId,
    this.objType,
    this.independent,
    this.certainForSer,
  );
}
