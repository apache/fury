import 'package:analyzer/dart/element/element.dart';
import 'package:fury_core/src/code_gen/entity/either.dart';
import 'package:fury_core/src/code_gen/entity/location_mark.dart';
import 'package:fury_core/src/code_gen/meta/public_accessor_field.dart';

import '../../const/location_level.dart';
import '../../meta/impl/field_spec_immutable.dart';
import '../annotation/location_level_ensure.dart';

typedef FieldOverrideChecker = bool Function(String fieldName);

abstract class FieldAnalyser{
  Either<FieldSpecImmutable, PublicAccessorField>? analyse(
    FieldElement element,
    FieldOverrideChecker fieldOverrideChecker,
    @LocationEnsure(LocationLevel.clsLevel) LocationMark locationMark,
  );
}