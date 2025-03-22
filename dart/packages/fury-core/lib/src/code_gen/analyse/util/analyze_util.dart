import 'package:analyzer/dart/element/element.dart';
import 'package:fury_core/src/code_gen/collection/key/declare_simple_info.dart';

class AnalyseUtil{
  static DeclareSimpleInfo getLibKey(ConstructorElement element){
    return DeclareSimpleInfo(element.librarySource.fullName, element.enclosingElement3.displayName);
  }
}