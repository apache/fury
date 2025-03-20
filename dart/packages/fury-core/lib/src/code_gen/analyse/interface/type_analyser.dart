import 'package:fury_core/src/code_gen/entity/location_mark.dart';

import '../../const/location_level.dart';
import '../../meta/impl/type_spec_gen.dart';
import '../annotation/location_level_ensure.dart';
import '../entity/analysis_res_wrap.dart';

abstract class TypeAnalyser{

  TypeSpecGen getTypeImmutableAndTag(
    TypeDecision typeDecision,
    @LocationEnsure(LocationLevel.fieldLevel) LocationMark locationMark,
  );
}