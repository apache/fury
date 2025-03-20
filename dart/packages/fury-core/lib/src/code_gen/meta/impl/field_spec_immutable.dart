import 'package:fury_core/src/code_gen/meta/impl/type_adapter.dart';
import 'package:fury_core/src/code_gen/meta/impl/type_spec_gen.dart';
import 'package:fury_core/src/code_gen/meta/lib_import_pack.dart';

import '../../config/gen_code_style.dart';
import '../../tool/gen_code_tool.dart';
import '../gen_export.dart';

// 不会混入static
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

  // 这两个字段只是暂时nullable, 分析完成后才能确定下来
  // 只能等所有的Field都分析完了之后才能确定
  // 如果这里isPublic为真，这两个字段都应该为真
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

  // 这两个方法一定是在设置好之后的步骤才访问的!
  bool get canSet => _canSet!;
  bool get canGet => _canGet!;

  // String getFullTypeName(LibImportPack imports) => typeSpec.getFullName(imports);

  void _judgeAccessUsingByDeclaration() {
    if (!isPublic) return; // 这时只能通过全部成员分析完成后，通过setters 和 getters来判断
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
    int totalIndent = indentLevel * GenCodeStyle.indent;
    int nextTotalIndent = totalIndent + GenCodeStyle.indent;

    // class declaration part
    GenCodeTool.writeIndent(buf, totalIndent);
    buf.write("FieldSpec(\n");

    // FuryFieldSpec::name part
    GenCodeTool.writeIndent(buf, nextTotalIndent);
    buf.write("'");
    buf.write(name);
    buf.write("',\n");

    // FuryFieldSpec::type part
    typeSpec.genCodeReqImportsInfo(buf, imports, dartCorePrefixWithPoint, indentLevel + 1);

    // FuryFieldSpec::includeFromFury part
    GenCodeTool.writeIndent(buf, nextTotalIndent);
    buf.write(includeFromFury ? "true,\n" : "false,\n");
    // FuryFieldSpec::includeToFury part
    GenCodeTool.writeIndent(buf, nextTotalIndent);
    buf.write(includeToFury ? "true,\n" : "false,\n");

    // FuryFieldSpec::getter part
    GenCodeTool.writeIndent(buf, nextTotalIndent);
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
    GenCodeTool.writeIndent(buf, nextTotalIndent);
    if (includeFromFury && canSet){
      // 为什么includeFromFury后canSet仍然可能为false? 因为还有使用构造函数进行初始化的可能，此时canSet=false也是合法的
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
    GenCodeTool.writeIndent(buf, totalIndent);
    buf.write("),\n");
  }
}