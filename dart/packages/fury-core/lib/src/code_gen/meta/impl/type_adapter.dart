import 'package:fury_core/src/code_gen/meta/gen_export.dart';
import 'package:fury_core/src/code_gen/meta/impl/type_spec_gen.dart';
import 'package:fury_core/src/code_gen/meta/lib_import_pack.dart';
import 'package:fury_core/src/code_gen/tool/gen_code_tool.dart';
import '../../../const/obj_type.dart';
import '../../config/gen_code_style.dart';

final class TypeAdapter extends GenExport{
  final TypeSpecGen typeSpec;
  TypeAdapter(this.typeSpec);

  void _genCodeInner(
    StringBuffer buf,
    LibImportPack imports,
    TypeSpecGen spec,
    String paramName,
    [String? dartCorePrefixWithPoint]
  ) {
    ObjType objType = spec.immutablePart.objType;
    // 这里就没办法兼容LinkedList， 因为它没有实现dart的List接口，当要支持LinkedList，这里也需要改动
    if (objType == ObjType.LIST || objType == ObjType.SET){
      if (spec.nullable){
        buf.write('($paramName == null) ? null : ');
      }
      buf.write(spec.getFullNameNoLastNull(imports));
      buf.write('.of((');
      buf.write(paramName);
      buf.write(' as ');
      buf.write(spec.getShortName(imports));
      buf.write(').map((v)=>');
      _genCodeInner(buf, imports, spec.genericsArgs[0],'v', dartCorePrefixWithPoint);
      buf.write(')');
      buf.write(')');
      return;
    }
    if (objType == ObjType.MAP){
      if (spec.nullable){
        buf.write('($paramName == null) ? null : ');
      }
      buf.write(spec.getFullNameNoLastNull(imports));
      buf.write('.of(');
      buf.write('(');
      buf.write(paramName);
      buf.write(' as ');
      buf.write(spec.getShortName(imports));
      buf.write(').map((k,v)=>');
      if (dartCorePrefixWithPoint!=null){
        buf.write(dartCorePrefixWithPoint);
      }
      buf.write('MapEntry(');
      _genCodeInner(buf, imports, spec.genericsArgs[0],'k',dartCorePrefixWithPoint);
      buf.write(',');
      _genCodeInner(buf, imports, spec.genericsArgs[1],'v', dartCorePrefixWithPoint);
      buf.write(')');
      buf.write(')');
      buf.write(')');
      return;
    }else{
      buf.write('(');
      buf.write(paramName);
      buf.write(' as ');
      buf.write(spec.getShortName(imports));
      if (spec.nullable){
        buf.write('?');
      }
      buf.write(')');
    }
  }

  @override
  void genCodeReqImportsInfo(
    StringBuffer buf,
    LibImportPack imports,
    String? dartCorePrefixWithPoint,
    [int indentLevel = 0, String paramName = 'v']
  ) {
    int totalIndent = indentLevel * GenCodeStyle.indent;
    GenCodeTool.writeIndent(buf, totalIndent);
    _genCodeInner(buf, imports, typeSpec, paramName, dartCorePrefixWithPoint);
  }
}