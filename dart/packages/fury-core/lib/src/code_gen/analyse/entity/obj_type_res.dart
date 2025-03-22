import '../../../const/obj_type.dart';

class ObjTypeRes{
  static const namedEnum = ObjTypeRes(ObjType.NAMED_ENUM, true,);
  static const namedStruct = ObjTypeRes(ObjType.NAMED_STRUCT, false);
  static const unknownStruct = ObjTypeRes(ObjType.UNKNOWN_YET, false);

  final ObjType objType; // null 表示明确不支持
  final bool certainForSer;

  const ObjTypeRes(this.objType, this.certainForSer);
}