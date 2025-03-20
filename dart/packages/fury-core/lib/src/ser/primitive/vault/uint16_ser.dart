// import 'package:fury_core/src/memory/byte_reader.dart';
// import 'package:fury_core/src/memory/byte_writer.dart';
// import 'package:fury_core/src/ser/ser.dart';
//
// import '../../../config/deser_pack.dart';
// import '../../../const/obj_type.dart';
//
// // dart 无uint16, 用户通过注解指定将dart中的int转换为uint8, 故可能出现范围错误
// class Uint16Ser extends Ser<int>{
//
//   const Uint16Ser(): super(ObjType.UINT16);
//
//   @override
//   int read(ByteReader br, DeserConfig conf) {
//     // 貌似还不支持此类型，go版本直接不支持， java版本的实现貌似也有问题
//     throw UnimplementedError();
//   }
//
//   @override
//   void write(ByteWriter bd, int value) {
//     throw UnimplementedError();
//   }
// }