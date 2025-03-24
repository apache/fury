import 'package:analyzer/dart/element/element.dart';
import 'package:analyzer/dart/element/visitor.dart';
import 'package:fury/src/codegen/analyze/analyzer.dart';
import 'package:fury/src/codegen/analyze/annotation/location_level_ensure.dart';
import 'package:fury/src/codegen/const/location_level.dart';
import 'package:fury/src/codegen/entity/either.dart';
import 'package:fury/src/codegen/entity/location_mark.dart';
import 'package:fury/src/codegen/meta/impl/field_spec_immutable.dart';
import 'package:fury/src/codegen/meta/public_accessor_field.dart';

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
    Either<FieldSpecImmutable, PublicAccessorField>? field = Analyzer.fieldAnalyzer.analyze(
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