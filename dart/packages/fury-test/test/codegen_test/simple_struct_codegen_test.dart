// @Skip()
library;


import 'package:collection/collection.dart';
import 'package:analyzer/dart/element/element.dart';
import 'package:analyzer/dart/element/type.dart';
import 'package:build/build.dart';
import 'package:build_test/build_test.dart';
import 'package:checks/checks.dart';
import 'package:test/test.dart';

void main(){
  group('Simple Struct Code Generation Tests', () {
    test('should verify code generation for a simple enum', () async {
      // await runBuild();
      AssetId inputId = AssetId('fury-test', 'lib/entity/enum_foo.dart');
      var lib = await resolveAsset(inputId, (resolver) async {
        return resolver.libraryFor(inputId);
      });
      List<String> variables = [];
      for (var libPart in lib.children){
        for (var ele in libPart.children){
          if (ele is VariableElement){
            InterfaceType type = ele.type as InterfaceType;
            if (type.element.name == 'EnumSpec'){
              // print('found EnumSpec: ${ele.name}');
              variables.add(ele.name);
            }
          }
        }
      }
      check(variables.equals(['\$EnumFoo', '\$EnumSubClass'])).isTrue();
    });

    test('should validate code presence for a simple struct', () async {
      // await runBuild();
      AssetId inputId = AssetId('fury-test', 'lib/entity/time_obj.dart');
      var lib = await resolveAsset(inputId, (resolver) async {
        return resolver.libraryFor(inputId);
      });
      List<String> variables = [];
      List<String> mixins = [];
      for (var libPart in lib.children){
        for (var ele in libPart.children){
          if (ele is VariableElement){
            InterfaceType type = ele.type as InterfaceType;
            if (type.element.name == 'ClassSpec'){
              // print('found EnumSpec: ${ele.name}');
              variables.add(ele.name);
            }
          }else if (ele is MixinElement){
            mixins.add(ele.name);
          }
        }
      }
      check(variables.equals(['\$TimeObj',])).isTrue();
      check(mixins).contains('_\$TimeObjFury');
    });

  });
}