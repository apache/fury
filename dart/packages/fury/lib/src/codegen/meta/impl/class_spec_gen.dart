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

import 'package:meta/meta.dart';
import 'package:fury/src/codegen/meta/custom_type_spec_gen.dart';
import 'package:fury/src/codegen/meta/impl/constructor_spec_gen.dart';
import 'package:fury/src/codegen/meta/impl/fields_spec_gen.dart';
import 'package:fury/src/codegen/meta/lib_import_pack.dart';
import 'package:fury/src/codegen/meta/impl/constructor_info.dart';
import 'package:fury/src/codegen/config/codegen_style.dart';
import 'package:fury/src/codegen/tool/codegen_tool.dart';

@immutable
class ClassSpecGen extends CustomTypeSpecGen{
  final bool promiseAcyclic;
  final bool noCyclicRisk;
  final FieldsSpecGen _fieldsSpecGen;
  final LibImportPack imports;
  late final ConstructorSpecGen _consSpecGen;
  late final String _varName;

  late final String? _dartCorePrefixWithPoint;

  ClassSpecGen(
    super.name,
    super.importPath,
    this.promiseAcyclic,
    this.noCyclicRisk,
    this._fieldsSpecGen,
    this.imports,
    ConstructorInfo consInfo,
  ){
    _varName = "\$$name";
    _dartCorePrefixWithPoint = imports.dartCorePrefix != null ? "${imports.dartCorePrefix}." : null;
    _consSpecGen = ConstructorSpecGen(
      name,
      imports,
      consInfo,
      _fieldsSpecGen,
    );
  }

  void _genMixinPart(StringBuffer buf){
    buf.write("mixin ");
    buf.write('_\$');
    buf.write(name);
    buf.write("Fury");
    buf.write(" implements Furiable {\n");
    CodegenTool.writeIndent(buf, CodegenStyle.indent);
    buf.write("@override\n");
    CodegenTool.writeIndent(buf, CodegenStyle.indent);
    buf.write("Type get \$furyType => ");
    buf.write(name);
    buf.write(";\n");
    buf.write("}\n");
  }

  @override
  void genCode(StringBuffer buf,[int indentLevel = 0]) {
    int totalIndent = indentLevel * CodegenStyle.indent;
    int nextTotalIndent = totalIndent + CodegenStyle.indent;
    // // the import part
    // buf.write("import 'packages:");
    // buf.write(FuryCoreConst.importPath);
    // buf.write("';\n");
    // buf.write(GenCodeStyle.magicSign);
    // buf.write(_varName);
    // buf.write(GenCodeStyle.markSep);
    // buf.write(name);
    // buf.write(GenCodeStyle.markSep);
    // buf.write(tag);
    // buf.write(GenCodeStyle.markSep);
    // buf.write(importPath);
    // buf.write("\n");

    // the declare of variable
    buf.write("final ");
    buf.write(_varName);
    buf.write(" = ClassSpec(\n");

    // arg: type name
    CodegenTool.writeIndent(buf, nextTotalIndent);
    buf.write(name);
    buf.write(",\n");

    // arg: type name
    CodegenTool.writeIndent(buf, nextTotalIndent);
    buf.write(promiseAcyclic ? "true" : "false");
    buf.write(",\n");

    // arg: noCyclicRisk
    CodegenTool.writeIndent(buf, nextTotalIndent);
    buf.write(noCyclicRisk ? "true" : "false");
    buf.write(",\n");

    // // second arg: tag
    // GenCodeTool.writeIndent(buf, nextTotalIndent);
    // buf.write("\'\'\'");
    // buf.write(tag);
    // buf.write("\'\'\',\n");

    // // fromFuryHash
    // GenCodeTool.writeIndent(buf, nextTotalIndent);
    // buf.write(_fromFuryHash);
    // buf.write(",\n");
    //
    // // toFuryHash
    // GenCodeTool.writeIndent(buf, nextTotalIndent);
    // buf.write(_toFuryHash);
    // buf.write(",\n");

    // arg: fields
    _fieldsSpecGen.genCodeReqImportsInfo(buf, imports, _dartCorePrefixWithPoint, indentLevel + 1);

    // arg: construct function
    _consSpecGen.genCodeReqImportsInfo(buf, imports, _dartCorePrefixWithPoint, indentLevel + 1);

    // tail part
    buf.write(");\n\n");

    _genMixinPart(buf);
  }
}