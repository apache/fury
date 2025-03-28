import 'package:collection/collection.dart';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/meta/specs/custom_type_spec.dart';
import 'package:fury/src/meta/specs/field_spec.dart';

typedef HasArgsCons = Object Function(List<Object?>);
typedef NoArgsCons = Object Function();

class ClassSpec extends CustomTypeSpec{
  final List<FieldSpec> fields;
  final bool promiseAcyclic;
  final bool noCyclicRisk; // No risk of cyclic references obtained by analyzing the code, for example, all fields are
  final HasArgsCons? construct;
  // Think, is noArgs really necessary? If a constructor with only basic type parameters is provided, is it acceptable?
  // No, during the deserialization phase, the order of fields is not arbitrary, it follows the field order (basic types are not necessarily at the beginning of the order)
  final NoArgsCons? noArgConstruct;

  ClassSpec(
    Type dartType,
    this.promiseAcyclic,
    this.noCyclicRisk,
    this.fields,
    this.construct,
    this.noArgConstruct,
  ): super(dartType, ObjType.NAMED_STRUCT) // Currently, a class can only be a named structure
  {
    assert(construct != null || noArgConstruct != null, 'construct and noArgConstruct can not be both non-null');
    // If noArgConstruct is null, then promiseAcyclic is required, A->B is equivalent to !A || B
    assert(noArgConstruct != null || promiseAcyclic || noCyclicRisk);
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
      (other is ClassSpec &&
        runtimeType == other.runtimeType &&
        fields.equals(other.fields) &&
        promiseAcyclic == other.promiseAcyclic &&
        noCyclicRisk == other.noCyclicRisk &&
        ((identical(construct, other.construct)) || (construct == null)==(construct == null)) &&
        ((identical(noArgConstruct, other.noArgConstruct)) || (noArgConstruct == null)==(other.noArgConstruct == null))
      );
  }

}
