import 'dart:collection';

import 'package:analyzer/dart/element/element.dart';
import 'package:fury/src/codegen/analyze/analysis_cache.dart';
import 'package:fury/src/codegen/analyze/analysis_type_identifier.dart';
import 'package:fury/src/codegen/meta/lib_import_pack.dart';

class ImportsAnalyzer{
  const ImportsAnalyzer();

  LibImportPack analyze(LibraryElement libElement){
    LibImportPack? importPack;
    // check cache
    importPack = AnalysisCache.getLibImport(libElement.id);
    if (importPack != null) return importPack;

    Map<int, String> libToPrefix = HashMap();
    List<LibraryImportElement> imports = libElement.definingCompilationUnit.libraryImports;
    String? prefix;
    for (var import in imports){
      prefix = import.prefix?.element.name;
      if (prefix == null) continue;
      libToPrefix[import.importedLibrary!.id] = prefix;
    }
    String? dartCorePrefix = libToPrefix[AnalysisTypeIdentifier.dartCoreLibId];
    // cache it
    importPack = LibImportPack(libToPrefix, dartCorePrefix);
    AnalysisCache.putLibImport(libElement.id, importPack);
    return importPack;
  }
}