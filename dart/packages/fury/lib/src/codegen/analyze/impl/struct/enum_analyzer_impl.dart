import 'package:analyzer/dart/element/element.dart';
import 'package:fury/src/codegen/analyze/interface/enum_analyzer.dart';
import 'package:fury/src/codegen/meta/impl/enum_spec_gen.dart';

class EnumAnalyzerImpl implements EnumAnalyzer{

  const EnumAnalyzerImpl();

  @override
  EnumSpecGen analyze(EnumElement enumElement) {
    String packageName = enumElement.location!.components[0];

    List<String> enumValues = enumElement.fields
    .where((e) => e.isEnumConstant)
    .map((e) => e.name)
    .toList();

    return EnumSpecGen(
      enumElement.name,
      packageName,
      enumValues,
    );
  }
}