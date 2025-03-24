import 'package:analyzer/dart/element/element.dart';
import 'package:fury/src/codegen/collection/key/declare_simple_info.dart';

class AnalyzeUtil{
  static DeclareSimpleInfo getLibKey(ConstructorElement element){
    return DeclareSimpleInfo(element.librarySource.fullName, element.enclosingElement3.displayName);
  }
}