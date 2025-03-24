import 'package:build/build.dart';
import 'package:fury/src/codegen/const/fury_const.dart';
import 'package:source_gen/source_gen.dart';

import 'obj_spec_generator.dart';

Builder furyObjSpecBuilder(BuilderOptions options) {
  return SharedPartBuilder(
    [ObjSpecGenerator()],  // 提供生成器
    FuryConst.intermediateMark,     // 输出文件的名称
  );
}