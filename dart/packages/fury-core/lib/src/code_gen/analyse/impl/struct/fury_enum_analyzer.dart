import 'package:analyzer/dart/element/element.dart';
import 'package:fury_core/src/code_gen/analyse/interface/enum_analyzer.dart';
import 'package:fury_core/src/code_gen/meta/impl/enum_spec_gen.dart';

class FuryEnumAnalyzer implements EnumAnalyzer{

  const FuryEnumAnalyzer();

  @override
  EnumSpecGen analyze(EnumElement enumElement) {
    String packageName = enumElement.location!.components[0];

    // LocationMark locationMark = LocationMark.clsLevel(
    //   packageName,
    //   enumElement.name,
    // );

    // FuryEnum furyEnum = Analyzer.furyEnumAnnotationAnalyzer.analyze(
    //   enumElement.metadata,
    //   locationMark,
    // );

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