import 'package:build/build.dart';
import 'package:fury_core/src/code_gen/const/fury_core_const.dart';
import 'package:source_gen/source_gen.dart';

import 'fury_obj_spec_generator.dart';

Builder furyObjSpecBuilder(BuilderOptions options) {
  return SharedPartBuilder(
    [FuryObjSpecGenerator()],  // 提供生成器
    FuryCoreConst.intermediateMark,     // 输出文件的名称
  );
}

// Builder furyEnumSpecBuilder(BuilderOptions options) {
//   return SharedPartBuilder(
//     [FuryEnumSpecGenerator()],  // 提供生成器
//     FuryCoreConst.intermediateMark,     // 输出文件的名称
//   );
// }

// Builder furyInitBuilder(BuilderOptions options) {
//   // make sure main-dart option is set
//   String? mainDart = options.config['main'];
//   if (mainDart == null) {
//     throw ArgumentError("[furyInitBuilder] option 'main' is required");
//   }
//   return FuryInitBuilder(mainDart);
// }

// Builder copyFileBuilder(BuilderOptions options) => CopyFileBuilder();
//
//
// /// 自定义 Builder
// class CopyFileBuilder implements Builder {
//   @override
//   final buildExtensions = const {
//     '.da{{}}t': ['lib/r.fu{{}}y.dart'], // 生成 .a.dart 文件
//   };
//
//   static bool gened = false;
//
//   @override
//   void build(BuildStep buildStep){
//
//     if (gened) return;
//     gened = true;
//
//     var it = buildStep.allowedOutputs;
//
//     var inputId = buildStep.inputId;
//
//     print('[CopyFileBuilder::build] inputId: $inputId');
//
//     // var outputId = inputId.changeExtension('.fury.dart');
//
//     var outputId = AssetId(buildStep.inputId.package, 'lib/r.fury.dart');
//
//     // Create a new target `AssetId` based on the old one.
//     // var copy = inputId.changeExtension('.copy.dart');
//
//     //print('@@@@@@@@@@@@@@@copy: $copy');
//     var contents = buildStep.readAsString(inputId);
//
//     buildStep.writeAsString(outputId, contents);
//   }
// }


// /// Reads a field element of type [List<String] and generates the merged content.
// class AddNamesGenerator extends MergingGenerator<List<String>, FuryMeta> {
//   /// Portion of source code included at the top of the generated file.
//   /// Should be specified as header when constructing the merging builder.
//   static String get header {
//     return '/// Added names.';
//   }
//
//   /// Portion of source code included at the very bottom of the generated file.
//   /// Should be specified as [footer] when constructing the merging builder.
//   static String get footer {
//     return '/// This is the footer.';
//   }
//
//   @override
//   List<String> generateStreamItemForAnnotatedElement(
//       Element element,
//       ConstantReader annotation,
//       BuildStep buildStep,
//       ) {
//     if (element is ClassElement) {
//       return ["class1, class2, class3"];
//     }
//     return ['element1', 'element2', 'element3'];
//   }
//
//   /// Returns the merged content.
//   @override
//   FutureOr<String> generateMergedContent(Stream<List<String>> stream) async {
//     final b = StringBuffer();
//     var i = 0;
//     final allNames = <List<String>>[];
//     // Iterate over stream:
//     await for (final names in stream) {
//       b.write('final name$i = [');
//       b.writeAll(names,  ',');
//       b.writeln('];');
//       ++i;
//       allNames.add(names);
//     }
//
//     b.writeln('');
//     b.writeln('final List<List<String>> names = [');
//     for (var names in allNames) {
//       b.writeln('  [');
//       b.writeAll(names, ',');
//       b.writeln('  ],');
//     }
//     b.writeln('];');
//     return b.toString();
//   }
// }
//
// /// Defines a merging builder.
// /// Honours the options: `input_files`, `output_file`, `header`, `footer`,
// /// and `sort_assets` that can be set in `build.yaml`.
// Builder addNamesBuilder(BuilderOptions options) {
//   BuilderOptions defaultOptions = BuilderOptions({
//     'input_files': 'lib/*.dart',
//     'output_file': 'lib/output.dart',
//     'header': AddNamesGenerator.header,
//     'footer': AddNamesGenerator.footer,
//     'sort_assets': true,
//   });
//
//   // Apply user set options.
//   options = defaultOptions.overrideWith(options);
//   return MergingBuilder<List<String>, LibDir>(
//     generator: AddNamesGenerator(),
//     inputFiles: options.config['input_files'],
//     outputFile: options.config['output_file'],
//     header: options.config['header'],
//     footer: options.config['footer'],
//     sortAssets: options.config['sort_assets'],
//   );
// }