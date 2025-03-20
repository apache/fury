import 'dart:collection';

import 'package:analyzer/dart/element/element.dart';
import 'package:fury_core/src/code_gen/analyse/analysis_type_identifier.dart';
// ignore: unused_import
import 'package:fury_core/src/code_gen/collection/key/lib_string_key.dart';
import 'package:fury_core/src/code_gen/meta/lib_import_pack.dart';

import '../../analysis_cache.dart';

class ImportsAnalyzer{
  const ImportsAnalyzer();

  // 目前没有参透以下id的，不知道一个触发的分析过程(可能是多个文件的分析过程)，type和library的id是否是恒定的， 如果是恒定的，那么就不用这么麻烦了(定义各种key)
  // 获取字段类型的元素
  // getImportPrefix(element, element.library);
  // (element as VariableElement).library?.definingCompilationUnit.libraryImports[1].prefix?.element.name;
  // (element as VariableElement).library?.definingCompilationUnit.libraryImports[2].importedLibrary?.id;
  // element.type.element?.library?.id;

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