// constructor spec generator

import 'package:fury_core/src/code_gen/entity/contructor_params.dart';
import 'package:fury_core/src/code_gen/meta/gen_export.dart';
import 'package:fury_core/src/code_gen/meta/impl/field_spec_immutable.dart';
import 'package:fury_core/src/code_gen/meta/impl/fields_spec_gen.dart';
import 'package:fury_core/src/code_gen/meta/lib_import_pack.dart';
import 'package:fury_core/src/code_gen/tool/gen_code_tool.dart';
import 'package:meta/meta.dart';

import '../../config/gen_code_style.dart';
import 'cons_info.dart';

@immutable
class ConsSpecGen extends GenExport{

  final String className;
  final LibImportPack imports;
  final ConsInfo consInfo;
  final FieldsSpecGen fieldsSpecGen;

  const ConsSpecGen(this.className, this.imports,this.consInfo, this.fieldsSpecGen);

  @override
  void genCodeReqImportsInfo(StringBuffer buf, LibImportPack imports, String? dartCorePrefixWithPoint,[int indentLevel = 0]) {
    int totalIndent = indentLevel * GenCodeStyle.indent;
    if (consInfo.flexibleOrUnnamedCons){
      // 使用无参
      _genCodeForFlexibleCons(buf, consInfo.flexibleConsName!, totalIndent);
    }else{
      // 使用无名
      _genCodeForUnnamedCons(buf, consInfo.unnamedConsParams!, totalIndent, dartCorePrefixWithPoint);
    }
  }

  void _genCodeForFlexibleCons(StringBuffer buf, String consName, int baseIndent){
    GenCodeTool.writeIndent(buf, baseIndent);
    buf.write("null,\n");
    GenCodeTool.writeIndent(buf, baseIndent);
    buf.write("() => ");
    buf.write(className);
    if (consName.isNotEmpty){
      buf.write(".");
      buf.write(consName);
    }
    buf.write("(),\n");
  }

  void _genCodeForUnnamedCons(StringBuffer buf, ConstructorParams consParams, int baseIndent, String? dartCorePrefixWithPoint){
    int nextTotalIndent = baseIndent + GenCodeStyle.indent;
    List<FieldSpecImmutable> fields = fieldsSpecGen.fields;
    List<bool> setThroughConsFlags = fieldsSpecGen.setThroughConsFlags;
    GenCodeTool.writeIndent(buf, baseIndent);

    buf.write("(");
    if (dartCorePrefixWithPoint != null) {
      buf.write(dartCorePrefixWithPoint);
    }
    buf.write("List<dynamic> objList) => ");
    buf.write(className);
    buf.write("(\n");
    for (var param in consParams.positional){
      if (param.fieldIndex == -1) continue;
      GenCodeTool.writeIndent(buf, nextTotalIndent);
      String paramName = "objList[${param.fieldIndex}]";
      fields[param.fieldIndex].typeAdapter.genCodeReqImportsInfo(buf, imports, dartCorePrefixWithPoint, 0 ,paramName);
      buf.write(",\n");
    }

    for (var param in consParams.named){
      if (param.fieldIndex == -1) continue;
      GenCodeTool.writeIndent(buf, nextTotalIndent);
      buf.write(param.name);
      buf.write(": ");
      String paramName = "objList[${param.fieldIndex}]";
      fields[param.fieldIndex].typeAdapter.genCodeReqImportsInfo(buf, imports, dartCorePrefixWithPoint, 0, paramName);
      buf.write(",\n");
    }
    GenCodeTool.writeIndent(buf, baseIndent);
    buf.write(')');

    late FieldSpecImmutable field;
    for (int i =0; i< fields.length; ++i){
      field = fields[i];
      if (field.includeFromFury && !setThroughConsFlags[i]){
        assert(field.canSet); // 这在前面的步骤应该就有保证，如果有错，早已经异常停止
        buf.write("..");
        buf.write(field.name);
        buf.write(" = ");
        String paramName = "objList[$i]";
        field.typeAdapter.genCodeReqImportsInfo(buf, imports, dartCorePrefixWithPoint, 0, paramName);
        buf.write("\n");
      }
    }
    buf.write(",\n");
    GenCodeTool.writeIndent(buf, baseIndent);
    buf.write("null,\n");
  }
}