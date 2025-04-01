import 'package:fury/src/const/obj_type.dart';
import 'package:analyzer/dart/element/type.dart';

typedef TypeDecision = ({InterfaceType type, bool forceNullable});

class ObjTypeWrapper{
  static const namedEnum = ObjTypeWrapper(ObjType.NAMED_ENUM, true,);
  static const namedStruct = ObjTypeWrapper(ObjType.NAMED_STRUCT, false);
  static const unknownStruct = ObjTypeWrapper(ObjType.UNKNOWN_YET, false);

  final ObjType objType; // null means unsupported
  final bool certainForSer;

  const ObjTypeWrapper(this.objType, this.certainForSer);
}