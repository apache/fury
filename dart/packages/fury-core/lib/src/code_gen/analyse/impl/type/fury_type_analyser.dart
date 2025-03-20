import 'package:analyzer/dart/element/element.dart';
import 'package:analyzer/dart/element/nullability_suffix.dart';
import 'package:analyzer/dart/element/type.dart';
import 'package:fury_core/src/code_gen/analyse/entity/obj_type_res.dart';
import 'package:fury_core/src/code_gen/entity/location_mark.dart';
import 'package:fury_core/src/code_gen/meta/impl/type_immutable.dart';

import '../../../const/location_level.dart';
import '../../../meta/impl/type_spec_gen.dart';
import '../../analysis_cache.dart';
import '../../analyzer.dart';
import '../../annotation/location_level_ensure.dart';
import '../../entity/analysis_res_wrap.dart';
import '../../interface/type_analyser.dart';

class FuryTypeAnalyser extends TypeAnalyser{

  FuryTypeAnalyser();

  TypeSpecGen _analyse(
    TypeDecision typeDecision,
    @LocationEnsure(LocationLevel.fieldLevel) LocationMark locationMark,
  ){
    assert (locationMark.ensureFieldLevel);
    InterfaceType type = typeDecision.type;
    bool nullable = (typeDecision.forceNullable) ? true:  type.nullabilitySuffix == NullabilitySuffix.question;
    TypeImmutable typeImmutable = _analyzeTypeImmutable(type.element, locationMark);

    List<TypeSpecGen> typeArgsTypes = [];
    for (DartType arg in type.typeArguments){
      typeArgsTypes.add(
        _analyse(Analyzer.typeSystemAnalyzer.decideInterfaceType(arg), locationMark),
      );
    }
    TypeSpecGen spec = TypeSpecGen(
      typeImmutable,
      nullable,
      typeArgsTypes,
    );
    return spec;
  }


  @override
  TypeSpecGen getTypeImmutableAndTag(
    TypeDecision typeDecision,
    @LocationEnsure(LocationLevel.fieldLevel) LocationMark locationMark,
  ){
    InterfaceType type = typeDecision.type;

    InterfaceElement element = type.element;
    int libId = element.library.id;
    bool nullable = (typeDecision.forceNullable) ? true:  type.nullabilitySuffix == NullabilitySuffix.question;

    ObjTypeRes objTypeRes = Analyzer.typeSystemAnalyzer.analyzeObjType(element, locationMark);
    List<TypeSpecGen> typeArgsTypes = [];
    for (DartType arg in type.typeArguments){
      typeArgsTypes.add(
        _analyse(
          Analyzer.typeSystemAnalyzer.decideInterfaceType(arg),
          locationMark,
        ),
      );
    }
    return TypeSpecGen(
      TypeImmutable(
        element.name,
        libId,
        objTypeRes.objType,
        objTypeRes.objType.independent,
        objTypeRes.certainForSer,
      ),
      nullable,
      typeArgsTypes,
    );
  }

  TypeImmutable _analyzeTypeImmutable(
    InterfaceElement element,
    @LocationEnsure(LocationLevel.fieldLevel) LocationMark locationMark,
  ){
    // loc
    assert (locationMark.ensureFieldLevel);
    // check cache
    TypeImmutable? typeImmutable = AnalysisCache.getTypeImmutable(element.id);
    if (typeImmutable != null) {
      return typeImmutable;
    }
    // step by step
    String name = element.name;

    ObjTypeRes objTypeRes = Analyzer.typeSystemAnalyzer.analyzeObjType(element, locationMark);
    int libId = element.library.id; // TODO: 是这个吗？需要运行时再看看
    // cache
    typeImmutable = TypeImmutable(
      name,
      libId,
      objTypeRes.objType,
      objTypeRes.objType.independent,
      objTypeRes.certainForSer,
    );
    AnalysisCache.putTypeImmutable(element.id, typeImmutable);
    return typeImmutable;
  }
}