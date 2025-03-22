import 'package:analyzer/dart/element/element.dart';
import 'package:analyzer/dart/element/type.dart';
import 'package:fury_core/fury_core.dart';
import 'package:fury_core/src/code_gen/analyse/analysis_type_identifier.dart';
import 'package:fury_core/src/code_gen/analyse/analyzer.dart';
import 'package:fury_core/src/code_gen/entity/fields_cache_unit.dart';
import 'package:fury_core/src/code_gen/entity/location_mark.dart';
import 'package:fury_core/src/code_gen/meta/impl/fields_spec_gen.dart';

import '../../../../annotation/fury_class.dart';
import '../../../meta/impl/class_spec_gen.dart';
import '../../../meta/impl/cons_info.dart';
import '../../../meta/lib_import_pack.dart';
import '../../interface/class_analyser.dart';

class FuryClassClassAnalyser implements ClassAnalyser{

  const FuryClassClassAnalyser();

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
    // clsElement.library.definingCompilationUnit.libraryImports[2].importedLibrary;
    // 这里的clsElement一定是ClassElement
    assert(clsElement.location != null && clsElement.location!.components.isNotEmpty, 'Class location is null or empty');
    /*------------------analyse package and class name-----------------------------*/
    String packageName = clsElement.location!.components[0];
    String className = clsElement.name;
    LocationMark locationMark = LocationMark.clsLevel(packageName, className);
    /*------------------analyse FuryMeta-----------------------------*/
    FuryClass furyClass = Analyzer.furyClassAnnotationAnalyzer.analyze(
      clsElement.metadata,
      clsElement.id,
      locationMark,
    );
    /*--------------analyze imports--------------------------------*/
    LibImportPack libImportPack = Analyzer.importsAnalyzer.analyze(clsElement.library);

    /*-----------------analyse fields------------------------------*/
    FieldsCacheUnit fieldsUnit = Analyzer.fieldsAnalyzer.analyseFields(clsElement, locationMark);

    /*--------------analyze constructor----------------------------*/
    ConsInfo consInfo = Analyzer.constructorAnalyzer.analyse(
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