import 'package:fury_core/src/const/obj_type.dart';
import 'package:meta/meta.dart';

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
