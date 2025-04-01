import 'package:analyzer/dart/element/element.dart';
import 'package:analyzer/dart/element/nullability_suffix.dart';
import 'package:analyzer/dart/element/type.dart';
import 'package:fury/src/codegen/analyze/analysis_cache.dart';
import 'package:fury/src/codegen/analyze/analysis_wrappers.dart';
import 'package:fury/src/codegen/analyze/analyzer.dart';
import 'package:fury/src/codegen/analyze/annotation/location_level_ensure.dart';
import 'package:fury/src/codegen/analyze/interface/type_analyzer.dart';
import 'package:fury/src/codegen/const/location_level.dart';
import 'package:fury/src/codegen/entity/location_mark.dart';
import 'package:fury/src/codegen/meta/impl/type_immutable.dart';
import 'package:fury/src/codegen/meta/impl/type_spec_gen.dart';

class TypeAnalyzerImpl extends TypeAnalyzer{

  TypeAnalyzerImpl();

  TypeSpecGen _analyze(
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
        _analyze(Analyzer.typeSystemAnalyzer.decideInterfaceType(arg), locationMark),
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

    ObjTypeWrapper objTypeRes = Analyzer.typeSystemAnalyzer.analyzeObjType(element, locationMark);
    List<TypeSpecGen> typeArgsTypes = [];
    for (DartType arg in type.typeArguments){
      typeArgsTypes.add(
        _analyze(
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

    ObjTypeWrapper objTypeRes = Analyzer.typeSystemAnalyzer.analyzeObjType(element, locationMark);
    int libId = element.library.id;
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