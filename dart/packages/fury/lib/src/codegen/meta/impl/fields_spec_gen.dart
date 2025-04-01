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
import 'package:fury/src/codegen/meta/impl/field_spec_immutable.dart';
import 'package:fury/src/codegen/meta/lib_import_pack.dart';
import 'package:fury/src/codegen/tool/codegen_tool.dart';

class FieldsSpecGen extends GenExport{
  final List<FieldSpecImmutable> fields;
  final List<bool> setThroughConsFlags;

  bool fieldSorted;
  FieldsSpecGen(this.fields, this.fieldSorted, this.setThroughConsFlags);

  // Iterable<FieldSpecImmutable> get fieldsIterator sync*{
  //   for (int i = 0; i < fields.length; ++i){
  //     if (setThroughConsFlags[i]){
  //       yield fields[i];
  //     }
  //   }
  // }
  //
  // bool isSetThroughCons(int index) => setThroughConsFlags[index];

  @override
  void genCodeReqImportsInfo(StringBuffer buf, LibImportPack imports, String? dartCorePrefixWithPoint,[int indentLevel = 0]) {
    int totalIndent = indentLevel * CodegenStyle.indent;
    CodegenTool.writeIndent(buf, totalIndent);
    buf.write("[\n");
    for (var field in fields){
      field.genCodeReqImportsInfo(buf, imports, dartCorePrefixWithPoint, indentLevel + 1,);
    }
    CodegenTool.writeIndent(buf, totalIndent);
    buf.write("],\n");
  }

}