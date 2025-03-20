import 'package:fury_core/src/code_gen/config/gen_code_style.dart';
import 'package:fury_core/src/code_gen/meta/custom_type_spec_gen.dart';
import 'package:fury_core/src/code_gen/meta/impl/cons_info.dart';
import 'package:fury_core/src/code_gen/meta/impl/cons_spec_gen.dart';
import 'package:fury_core/src/code_gen/meta/impl/fields_spec_gen.dart';
import 'package:fury_core/src/code_gen/meta/lib_import_pack.dart';
import 'package:meta/meta.dart';

import '../../tool/gen_code_tool.dart';

@immutable
class ClassSpecGen extends CustomTypeSpecGen{
  final bool promiseAcyclic;
  final bool noCyclicRisk;
  final FieldsSpecGen _fieldsSpecGen;
  final LibImportPack imports;
  late final ConsSpecGen _consSpecGen;
  late final String _varName;

  late final String? _dartCorePrefixWithPoint;

  ClassSpecGen(
    super.name,
    super.importPath,
    this.promiseAcyclic,
    this.noCyclicRisk,
    this._fieldsSpecGen,
    this.imports,
    ConsInfo consInfo,
  ){
    _varName = "\$$name";
    _dartCorePrefixWithPoint = imports.dartCorePrefix != null ? "${imports.dartCorePrefix}." : null;
    _consSpecGen = ConsSpecGen(
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
    buf.write(" implements Furable {\n");
    GenCodeTool.writeIndent(buf, GenCodeStyle.indent);
    buf.write("@override\n");
    GenCodeTool.writeIndent(buf, GenCodeStyle.indent);
    buf.write("Type get \$furyType => ");
    buf.write(name);
    buf.write(";\n");
    buf.write("}\n");
  }

  @override
  void genCode(StringBuffer buf,[int indentLevel = 0]) {
    int totalIndent = indentLevel * GenCodeStyle.indent;
    int nextTotalIndent = totalIndent + GenCodeStyle.indent;
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
    GenCodeTool.writeIndent(buf, nextTotalIndent);
    buf.write(name);
    buf.write(",\n");

    // arg: type name
    GenCodeTool.writeIndent(buf, nextTotalIndent);
    buf.write(promiseAcyclic ? "true" : "false");
    buf.write(",\n");

    // arg: noCyclicRisk
    GenCodeTool.writeIndent(buf, nextTotalIndent);
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