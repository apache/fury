import 'package:fury/src/codegen/config/codegen_style.dart';
import 'package:fury/src/codegen/meta/custom_type_spec_gen.dart';
import 'package:fury/src/codegen/tool/codegen_tool.dart';
import 'package:meta/meta.dart';

@immutable
class EnumSpecGen extends CustomTypeSpecGen{
  final List<String> _enumVarNames;
  late final String _varName;

  EnumSpecGen(super.name, super.importPath, this._enumVarNames){
    _varName = "\$$name";
    assert(_enumVarNames.isNotEmpty);
  }

  void _writeFieldsStr(StringBuffer buf, int indentLevel){
    int totalIndent = indentLevel * CodegenStyle.indent;
    CodegenTool.writeIndent(buf, totalIndent);
    buf.write("[");
    for (String varName in _enumVarNames){
      buf.write(name);
      buf.write(".");
      buf.write(varName);
      buf.write(', ');
    }
    buf.write("],\n");
  }

  @override
  void genCode(StringBuffer buf, [int indentLevel = 0]) {
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
    buf.write("const ");
    buf.write(_varName);
    buf.write(" = EnumSpec(\n");

    // type name
    CodegenTool.writeIndent(buf, CodegenStyle.indent * (indentLevel + 1));
    buf.write(name);
    buf.write(",\n");

    // // tag
    // GenCodeTool.writeIndent(buf, GenCodeStyle.indent * (indentLevel + 1));
    // buf.write("'''");
    // buf.write(tag);
    // buf.write("'''");
    // buf.write(",\n");

    _writeFieldsStr(buf, indentLevel + 1);

    // tail part
    buf.write(");\n");
  }
}