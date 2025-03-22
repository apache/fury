// import 'package:fury_core/src/excep/fury_exception.dart';
// import 'package:fury_core/src/memory/byte_reader.dart';
// import 'package:fury_core/src/memory/byte_writer.dart';
// import 'package:fury_core/src/ser/ser.dart';
//
// import '../../../config/deser_pack.dart';
// import '../../../const/obj_type.dart';
//
// // dart 无int8, 用户通过注解指定将dart中的int转换为int8, 故可能出现范围错误
// class Uint8Ser extends Ser<int>{
//
//   const Uint8Ser(): super(ObjType.UINT8);
//
//   @override
//   int read(ByteReader br, DeserConfig conf) {
//     return br.readUint8();// 使用有符号的8位整数，和FuryJava中的byte一致
//   }
//
//   @override
//   void write(ByteWriter bd, int value) {
//     if (value < 0 || value > 255){
//       throw FuryException.serRangeExcep(type, value);
//     }
//     bd.writeUint8(value);
//   }
// }