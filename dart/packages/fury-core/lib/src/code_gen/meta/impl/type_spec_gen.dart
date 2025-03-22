import 'package:fury_core/src/code_gen/meta/gen_export.dart';
import 'package:fury_core/src/code_gen/meta/impl/type_immutable.dart';
import 'package:fury_core/src/code_gen/meta/lib_import_pack.dart';

import '../../../const/obj_type.dart';
import '../../config/gen_code_style.dart';
import '../../tool/gen_code_tool.dart';

class TypeSpecGen extends GenExport{
  final TypeImmutable immutablePart;
  late bool nullable;
  final List<TypeSpecGen> genericsArgs; // 空表示没有泛型参数
  // final EnumBrief? enumBrief;

  String? _fullName;
  String? _fullNameNoLastNull;
  String? _shortName;

  TypeSpecGen(
    this.immutablePart,
    this.nullable,
    this.genericsArgs,
  );

  bool get independent => immutablePart.independent;

  String getFullName(LibImportPack imports) {
    if (_fullName != null) return _fullName!;
    _fullName = _equipFullTypeName(imports);
    return _fullName!;
  }

  String getFullNameNoLastNull(LibImportPack imports){
    if (_fullNameNoLastNull != null) return _fullNameNoLastNull!;
    if (!nullable) {
      return getFullName(imports);
    }
    _fullNameNoLastNull = _equipFullTypeName(imports, false);
    return _fullNameNoLastNull!;
  }

  String getShortName(LibImportPack imports) {
    if (_shortName != null) return _shortName!;
    String? prefix = imports.getPrefixByLibId(immutablePart.typeLibId);
    StringBuffer buf = StringBuffer();
    if (prefix != null){
      buf.write(prefix);
      buf.write(".");
    }
    buf.write(immutablePart.name);
    _shortName = buf.toString();
    return _shortName!;
  }

  String _equipFullTypeName(LibImportPack imports,[bool containLastNull = true]){
    String? importPrefix = imports.getPrefixByLibId(immutablePart.typeLibId);
    // print("equip full type name: $name");
    StringBuffer buf = StringBuffer();
    if (importPrefix != null){
      buf.write(importPrefix);
      buf.write(".");
    }
    buf.write(immutablePart.name);
    if (genericsArgs.isNotEmpty){
      buf.write("<");
      for (int i = 0; i < genericsArgs.length - 1; ++i){
        buf.write(genericsArgs[i].getFullName(imports));
        buf.write(", ");
      }
      buf.write(genericsArgs.last.getFullName(imports));
      buf.write(">");
    }
    if (nullable && containLastNull){
      buf.write("?");
    }
    return buf.toString();
  }

  @override
  void genCodeReqImportsInfo(StringBuffer buf, LibImportPack imports, String? dartCorePrefixWithPoint, [int indentLevel = 0]) {
    int totalIndent = indentLevel * GenCodeStyle.indent;
    int nextTotalIndent = totalIndent + GenCodeStyle.indent;

    // declare part
    GenCodeTool.writeIndent(buf, totalIndent);
    buf.write("TypeSpec(\n");

    // FuryTypeSpec::type
    String? prefix = imports.getPrefixByLibId(immutablePart.typeLibId);
    GenCodeTool.writeIndent(buf, nextTotalIndent);
    if (prefix != null){
      buf.write(prefix);
      buf.write(".");
    }
    buf.write(immutablePart.name);
    buf.write(",\n");

    // FuryTypeSpec::objType part
    GenCodeTool.writeIndent(buf, nextTotalIndent);
    buf.write("ObjType.");
    buf.write(immutablePart.objType.name);
    buf.write(",\n");

    // // FuryTypeSpec::type part
    // String? importPrefix = prefixReference.getPrefixByLibId(immutablePart.typeLibId);
    // GenCodeTool.writeIndent(buf, nextTotalIndent);
    // if (importPrefix != null){
    //   buf.write(importPrefix);
    //   buf.write(".");
    // }
    // buf.write(immutablePart.name);
    // buf.write(",\n");

    // FuryTypeSpec::nullable part
    GenCodeTool.writeIndent(buf, nextTotalIndent);
    buf.write(nullable ? "true,\n" : "false,\n");

    GenCodeTool.writeIndent(buf, nextTotalIndent);
    buf.write(immutablePart.certainForSer ? "true,\n" : "false,\n");

    if (immutablePart.objType != ObjType.NAMED_ENUM) {
      // FuryTypeSpec::enumSpec part
      GenCodeTool.writeIndent(buf, nextTotalIndent);
      buf.write("null,\n");
    }else {
      if (prefix != null){
        buf.write(prefix);
        buf.write(".");
      }
      buf.write('\$');
      buf.write(immutablePart.name);
      buf.write(",\n");
    }

    // FuryTypeSpec::genericsArgs part
    GenCodeTool.writeIndent(buf, nextTotalIndent);
    buf.write("const [\n");
    for (var arg in genericsArgs){
      arg.genCodeReqImportsInfo(buf, imports, dartCorePrefixWithPoint,indentLevel + 1);
    }
    GenCodeTool.writeIndent(buf, nextTotalIndent);
    buf.write("],\n");

    // end part
    GenCodeTool.writeIndent(buf, totalIndent);
    buf.write("),\n");
  }
}
