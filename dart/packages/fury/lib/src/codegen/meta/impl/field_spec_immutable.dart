/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import 'package:fury/src/codegen/config/codegen_style.dart';
import 'package:fury/src/codegen/meta/gen_export.dart';
import 'package:fury/src/codegen/meta/impl/type_adapter.dart';
import 'package:fury/src/codegen/meta/impl/type_spec_gen.dart';
import 'package:fury/src/codegen/meta/lib_import_pack.dart';
import 'package:fury/src/codegen/tool/codegen_tool.dart';

// Will not mix in static
class FieldSpecImmutable extends GenExport{
  final String name;

  final TypeSpecGen typeSpec;
  final TypeAdapter typeAdapter;

  final String className;

  final bool includeFromFury;
  final bool includeToFury;

  final bool isPublic;

  final bool isFinal;

  final bool isLate;

  final bool hasInitializer; // Whether the variable has an initializer at declaration.

  // These two fields are temporarily nullable, can only be determined after analysis is complete
  // Can only be determined after all Fields have been analyzed
  // If isPublic is true, both fields should be true
  bool? _canSet; // (public || have public setter) && (!(isFinal &&  hasInitializer))

  bool? _canGet; // public or have public getter

  String? transName;

  FieldSpecImmutable.publicOr(
    this.isPublic, {
      required this.name,
      required this.typeSpec,
      required this.className,
      required this.isFinal,
      required this.isLate,
      required this.hasInitializer,
      required this.includeFromFury,
      required this.includeToFury,
    }): typeAdapter = TypeAdapter(typeSpec){
    if (isPublic){
      assert(name.isNotEmpty && name[0] != "_");
    }
    _judgeAccessUsingByDeclaration();
  }

  bool get accessUnchangeable => _canSet != null && _canGet != null;

  void notifyHasSetter(bool hasSetter) {
    // assert(!isPublic);
    _canSet ??= hasSetter;
  }

  void notifyHasGetter(bool hasGetter) {
    // assert(!isPublic);
    _canGet ??= hasGetter;
  }

  // These two methods must only be accessed after the values are set!
  bool get canSet => _canSet!;
  bool get canGet => _canGet!;

  // String getFullTypeName(LibImportPack imports) => typeSpec.getFullName(imports);

  void _judgeAccessUsingByDeclaration() {
    if (!isPublic) return; // At this point, determination can only be made after analyzing all members, through setters and getters
    _canGet = true;
    if (isFinal){
      if(isLate){
        if (hasInitializer){
          _canSet = false;
        }else{
          _canSet = true;
        }
      }else{
        _canSet = false;
      }
    }else {
      _canSet = true;
    }
  }

  @override
  void genCodeReqImportsInfo(StringBuffer buf, LibImportPack imports, String? dartCorePrefixWithPoint,[int indentLevel = 0]) {
    int totalIndent = indentLevel * CodegenStyle.indent;
    int nextTotalIndent = totalIndent + CodegenStyle.indent;

    // class declaration part
    CodegenTool.writeIndent(buf, totalIndent);
    buf.write("FieldSpec(\n");

    // FuryFieldSpec::name part
    CodegenTool.writeIndent(buf, nextTotalIndent);
    buf.write("'");
    buf.write(name);
    buf.write("',\n");

    // FuryFieldSpec::type part
    typeSpec.genCodeReqImportsInfo(buf, imports, dartCorePrefixWithPoint, indentLevel + 1);

    // FuryFieldSpec::includeFromFury part
    CodegenTool.writeIndent(buf, nextTotalIndent);
    buf.write(includeFromFury ? "true,\n" : "false,\n");
    // FuryFieldSpec::includeToFury part
    CodegenTool.writeIndent(buf, nextTotalIndent);
    buf.write(includeToFury ? "true,\n" : "false,\n");

    // FuryFieldSpec::getter part
    CodegenTool.writeIndent(buf, nextTotalIndent);
    if (includeToFury){
      assert(canGet);
      buf.write("(${dartCorePrefixWithPoint ?? ''}Object inst) => (inst as ");
      buf.write(className);
      buf.write(").");
      buf.write(name);
      buf.write(",\n");
    }else {
      buf.write("null,\n");
    }

    // FuryFieldSpec::setter part
    CodegenTool.writeIndent(buf, nextTotalIndent);
    if (includeFromFury && canSet){
      // Why can canSet still be false after includeFromFury?
      // Because there is still the possibility of initialization using a constructor, in which case canSet=false is also valid.
      buf.write("(${dartCorePrefixWithPoint ?? ''}Object inst, var v) => (inst as ");
      buf.write(className);
      buf.write(").");
      buf.write(name);
      buf.write(" = ");
      typeAdapter.genCodeReqImportsInfo(buf, imports, dartCorePrefixWithPoint, 0);
      buf.write(",\n");
    }else {
      buf.write("null,\n");
    }

    // tail part
    CodegenTool.writeIndent(buf, totalIndent);
    buf.write("),\n");
  }
}
