// import 'package:fury_core/fury_core.dart';
//
// import '../../meta/class_spec.dart';
// import '../../meta/field_spec.dart';
// import '../../meta/fury_type_spec2.dart';
//
// class SomeClass{
//   int a;
//
//   SomeClass({
//     required this.a,
//   });
// }
//
// var $SomeClassFuryMeta = FuryClassSpec (
//   SomeClass,
//   "Person",
//   [
//     FuryFieldSpec(
//       'a',
//       FuryTypeSpec(
//         int,
//         false,
//         [
//           FuryTypeSpec(
//             int,
//             false,
//             [],
//           ),
//         ],
//       ),
//       (Object inst) => (inst as SomeClass).a,
//     ),
//   ],
//   (Map<String, Object> map) => SomeClass(
//     a: map['a'] as int,
//   ),
// );
//
//
// void func(){
//   FuryContext.init(
//     specs: [
//
//     ],
//   );
// }