import 'package:fury_core/src/const/obj_type.dart';

abstract class CustomTypeSpec{
  final Type dartType;
  final ObjType objType;
  const CustomTypeSpec(this.dartType, this.objType);
}