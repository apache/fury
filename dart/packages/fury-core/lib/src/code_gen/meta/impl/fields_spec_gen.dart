import 'package:fury_core/src/code_gen/meta/gen_export.dart';
import 'package:fury_core/src/code_gen/meta/impl/field_spec_immutable.dart';
import 'package:fury_core/src/code_gen/meta/lib_import_pack.dart';

import '../../tool/gen_code_tool.dart';
import '../../config/gen_code_style.dart';

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
  void genCodeReqImportsInfo(StringBuffer buf, LibImportPack imports, String? dartCoreImportPrefix,[int indentLevel = 0]) {
    int totalIndent = indentLevel * GenCodeStyle.indent;
    GenCodeTool.writeIndent(buf, totalIndent);
    buf.write("[\n");
    for (var field in fields){
      field.genCodeReqImportsInfo(buf, imports, dartCoreImportPrefix, indentLevel + 1,);
    }
    GenCodeTool.writeIndent(buf, totalIndent);
    buf.write("],\n");
  }

}