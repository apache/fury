// import 'package:fury_core/src/const/dart_type.dart';
// import 'package:fury_core/src/const/obj_type.dart';
//
// // 不支持带有泛型的类型，例如各种Collection
// enum FuryType{
//   int8(DartTypeEnum.FURYINT8, [DartTypeEnum.INT], [DartTypeEnum.INT]), // byte
//   int16(DartTypeEnum.FURYINT16, [DartTypeEnum.INT], [DartTypeEnum.INT]),
//   int32(DartTypeEnum.FURYINT32, [DartTypeEnum.INT], [DartTypeEnum.INT]),
//   float32(DartTypeEnum.FURYFLOAT32, [DartTypeEnum.DOUBLE], [DartTypeEnum.DOUBLE]),
//   date(DartTypeEnum.FURYDATE, [DartTypeEnum.DATETIME], [DartTypeEnum.DATETIME]);
//
//   final DartTypeEnum dartTypeEnum; // 这个类型的dartType
//   final List<DartTypeEnum> possibleTos; // 哪些dartType可以变为this来序列化
//   final List<DartTypeEnum> possibleFroms; // 哪些dartType可以接受this作为值
//
//   const FuryType(this.dartTypeEnum, this.possibleTos, this.possibleFroms);
//
//   // 不是这个方法笨，目前是要配合Dart Analyser的API, 但是也许有更好的API
//   static FuryType? fromString(String typeName){
//     switch(typeName){
//       case 'int8':
//         return int8;
//       case 'int16':
//         return int16;
//       case 'int32':
//         return int32;
//       case 'float32':
//         return float32;
//       case 'date':
//         return date;
//     }
//     return null;
//   }
//
//   bool canBeTargetOf (ObjType type){
//     for (var t in possibleTos){
//       if (t.defaultObjType == type){
//         return true;
//       }
//     }
//     return false;
//   }
//
//   bool canBeSourceFor (ObjType type){
//     for (var t in possibleFroms){
//       if (t.defaultObjType == type){
//         return true;
//       }
//     }
//     return false;
//   }
// }