import 'package:fury_core/src/code_gen/meta/impl/field_spec_immutable.dart';

class FieldsCacheUnit {
  final List<FieldSpecImmutable> fieldImmutables;
  final Set<String> fieldNames;
  final bool allFieldIndependent;
  bool fieldSorted = false;

  FieldsCacheUnit(this.fieldImmutables, this.allFieldIndependent, this.fieldNames);
}