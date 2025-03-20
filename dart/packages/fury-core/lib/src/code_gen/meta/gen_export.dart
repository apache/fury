import 'lib_import_pack.dart';

abstract class GenExport{
  const GenExport();
  void genCode(StringBuffer buf,[int indentLevel = 0]){
    throw UnimplementedError("genCode() is not implemented");
  }
  void genCodeReqImportsInfo(StringBuffer buf, LibImportPack imports, String? dartCorePrefixWithPoint,[int indentLevel = 0]){
    throw UnimplementedError("genCodeForType() is not implemented");
  }
}