// import 'package:fury_core/src/code_gen/analyse/field_analyser.dart';
// import 'package:fury_core/src/code_gen/analyse/impl/fury_field_analyser.dart';
// import 'package:fury_core/src/code_gen/analyse/impl/fury_spec_sum_analyser.dart';
// import 'package:fury_core/src/code_gen/analyse/spec_sum_analyser.dart';
//
// import '../analyse/class_analyser.dart';
// import '../analyse/impl/fury_class_analyser.dart';
// import '../analyse/impl/fury_type_analyser.dart';
// import '../analyse/type_analyser.dart';
//
// class InjectMan {
//   // 存储单例对象的映射，使用 Type 作为键，实例作为值
//   static Map<Type, Object>? _instances;
//
//   // 获取单例
//   static T get<T>() {
//     if (_instances == null) {
//       _registerAllSingletons();
//     }
//     final instance = _instances![T];
//     assert (instance != null, 'Instance of $T is not registered');
//     return instance as T;
//   }
//
//   // // 注册单例
//   // void _registerSingleton<T>(T instance) {
//   //   _instances[T] = instance as Object;
//   // }
//
//   static void _registerAllSingletons() {
//     _instances = {
//       ClassAnalyser: FuryClassAnalyser.inst,
//       FieldAnalyser: FuryFieldAnalyser.inst,
//       TypeAnalyser: FuryTypeAnalyser.inst,
//       SpecSumAnalyser: FurySpecSumAnalyser.inst,
//     };
//   }
//
// }
