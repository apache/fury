import 'package:fury_core/src/code_gen/excep/class_level_excep.dart';
import 'package:fury_core/src/code_gen/rules/code_rules.dart';

class ConstructorParamInformal extends ClassLevelExcep {

  final List<String> _invalidParams;

  // 没必要加reason字段了，因为知道reason实际上就是invalidParams
  ConstructorParamInformal(
      String libPath,
      String className,
      this._invalidParams,
      [String? where]): super(libPath, className, where);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write(CodeRules.consParamsOnlySupportThisAndSuper);
    buf.write('invalidParams: ');
    buf.writeAll(_invalidParams, ', ');
    buf.write('\n');
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}