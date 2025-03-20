import 'package:fury_core/src/code_gen/meta/custom_type_spec_gen.dart';
import 'package:meta/meta.dart';

import '../../config/gen_code_style.dart';
import '../../tool/gen_code_tool.dart';

@immutable
class EnumSpecGen extends CustomTypeSpecGen{
  final List<String> _enumVarNames;
  // final String tag;
  late final String _varName;

  EnumSpecGen(super.name, super.importPath, /*this.tag,*/ this._enumVarNames){
    _varName = "\$$name";
    assert(_enumVarNames.isNotEmpty);
  }

  void _writeFieldsStr(StringBuffer buf, int indentLevel){
    int totalIndent = indentLevel * GenCodeStyle.indent;
    GenCodeTool.writeIndent(buf, totalIndent);
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
    GenCodeTool.writeIndent(buf, GenCodeStyle.indent * (indentLevel + 1));
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