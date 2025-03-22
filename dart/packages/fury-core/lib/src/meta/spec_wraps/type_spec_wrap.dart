import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/meta/specs/field_spec.dart';
import 'package:fury_core/src/meta/specs/type_spec.dart';

import '../../ser/ser.dart' show Ser;

class TypeSpecWrap{
  final Type type;
  final ObjType objType;
  final bool certainForSer;
  final bool nullable;
  final List<TypeSpecWrap> genericsArgs;
  Ser? ser;

  TypeSpecWrap._(
    this.type,
    this.objType,
    this.certainForSer,
    this.nullable,
    this.genericsArgs,
    this.ser,
  );

  factory TypeSpecWrap.of(
    TypeSpec typeSpec
  ){
    List<TypeSpecWrap> genericsWraps = [];
    var genericsArgs = typeSpec.genericsArgs;
    for (int i = 0; i< genericsArgs.length; ++i) {
      TypeSpecWrap argWrap = TypeSpecWrap.of(typeSpec.genericsArgs[i]);
      genericsWraps.add(argWrap);
    }
    return TypeSpecWrap._(
      typeSpec.type,
      typeSpec.objType,
      typeSpec.certainForSer,
      typeSpec.nullable,
      genericsWraps,
      null,
    );
  }

  static List<TypeSpecWrap> ofList(
    List<FieldSpec> fieldSpecs
  ){
    List<TypeSpecWrap> typeSpecWraps = [];
    for (int i = 0; i< fieldSpecs.length; ++i) {
      TypeSpecWrap typeSpecWrap = TypeSpecWrap.of(fieldSpecs[i].typeSpec);
      typeSpecWraps.add(typeSpecWrap);
    }
    return typeSpecWraps;
  }

  bool get hasGenericsParam => genericsArgs.isNotEmpty;

  TypeSpecWrap? get param0 => genericsArgs.isNotEmpty ? genericsArgs[0] : null;
  TypeSpecWrap? get param1 => genericsArgs.length > 1 ? genericsArgs[1] : null;

  int get paramCount => genericsArgs.length;
}