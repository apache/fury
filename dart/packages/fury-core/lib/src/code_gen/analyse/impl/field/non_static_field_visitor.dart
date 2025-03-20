import 'package:analyzer/dart/element/element.dart';
import 'package:analyzer/dart/element/visitor.dart';

import '../../../const/location_level.dart';
import '../../../entity/either.dart';
import '../../../entity/location_mark.dart';
import '../../../meta/impl/field_spec_immutable.dart';
import '../../../meta/public_accessor_field.dart';
import '../../analyzer.dart';
import '../../annotation/location_level_ensure.dart';

class NonStaticFieldVisitor extends SimpleElementVisitor {
  final LocationMark _locationMark;
  final Set<String>? _superParamNames;

  final List<FieldSpecImmutable> fields = [];
  final List<PublicAccessorField> accessors = [];

  NonStaticFieldVisitor(
    this._superParamNames,
    @LocationEnsure(LocationLevel.clsLevel) this._locationMark,
  ){
    assert(_locationMark.ensureClassLevel);
  }

  bool _checkFieldOverride(String fieldName){
    return (_superParamNames != null && _superParamNames.contains(fieldName));
  }

  @override
  visitFieldElement(FieldElement element) {
    element.type.element?.library;
    if (element.isStatic) return;
    Either<FieldSpecImmutable, PublicAccessorField>? field = Analyzer.fieldAnalyser.analyse(
      element,
      _checkFieldOverride,
      _locationMark,
    );
    if (field == null) return;
    if (field.isLeft){
      fields.add(field.left!);
    } else {
      accessors.add(field.right!);
    }
  }
}