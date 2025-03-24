import 'package:analyzer/dart/element/element.dart';
import 'package:analyzer/dart/element/type.dart';
import 'package:fury/src/codegen/analyze/analysis_type_identifier.dart';
import 'package:fury/src/codegen/analyze/analyzer.dart';
import 'package:fury/src/codegen/analyze/interface/class_analyzer.dart';
import 'package:fury/src/codegen/entity/fields_cache_unit.dart';
import 'package:fury/src/codegen/entity/location_mark.dart';
import 'package:fury/src/codegen/meta/impl/class_spec_gen.dart';
import 'package:fury/src/annotation/fury_class.dart';
import 'package:fury/src/codegen/meta/impl/constructor_info.dart';
import 'package:fury/src/codegen/meta/impl/fields_spec_gen.dart';
import 'package:fury/src/codegen/meta/lib_import_pack.dart';

class ClassAnalyzerImpl implements ClassAnalyzer{

  const ClassAnalyzerImpl();

  void _setObjectTypeIfNeed(ClassElement clsElement){
    if (AnalysisTypeIdentifier.objectTypeSet) return;
    InterfaceType? type = clsElement.thisType;
    while (type!.superclass != null){
      type = type.superclass;
    }
    AnalysisTypeIdentifier.setObjectType = type;
  }

  @override
  ClassSpecGen analyze(ClassElement clsElement) {
    _setObjectTypeIfNeed(clsElement);
    // here clsElement is ClassElement
    assert(clsElement.location != null && clsElement.location!.components.isNotEmpty, 'Class location is null or empty');
    /*------------------analyze package and class name-----------------------------*/
    String packageName = clsElement.location!.components[0];
    String className = clsElement.name;
    LocationMark locationMark = LocationMark.clsLevel(packageName, className);
    /*------------------analyze FuryMeta-----------------------------*/
    FuryClass furyClass = Analyzer.classAnnotationAnalyzer.analyze(
      clsElement.metadata,
      clsElement.id,
      locationMark,
    );
    /*--------------analyze imports--------------------------------*/
    LibImportPack libImportPack = Analyzer.importsAnalyzer.analyze(clsElement.library);

    /*-----------------analyze fields------------------------------*/
    FieldsCacheUnit fieldsUnit = Analyzer.fieldsAnalyzer.analyzeFields(clsElement, locationMark);

    /*--------------analyze constructor----------------------------*/
    ConstructorInfo consInfo = Analyzer.constructorAnalyzer.analyze(
      clsElement.constructors,
      clsElement.id,
      furyClass.promiseAcyclic,
      fieldsUnit.allFieldIndependent,
      locationMark,
    );

    FieldsSpecGen fieldsSpecGen = Analyzer.accessInfoAnalyzer.checkAndSetTheAccessInfo(consInfo.unnamedConsParams, fieldsUnit.fieldImmutables, false, locationMark);

    if (!fieldsSpecGen.fieldSorted){
      Analyzer.fieldsSorter.sortFieldsByName(fieldsSpecGen.fields);
      fieldsSpecGen.fieldSorted = true;
    }

    return ClassSpecGen(
      className,
      packageName,
      furyClass.promiseAcyclic,
      fieldsUnit.allFieldIndependent,
      fieldsSpecGen,
      libImportPack,
      consInfo,
    );
  }
}