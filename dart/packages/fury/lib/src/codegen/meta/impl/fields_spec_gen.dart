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