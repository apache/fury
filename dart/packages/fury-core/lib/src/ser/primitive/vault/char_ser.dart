// import 'package:fury_core/src/config/fury_config.dart';
// import 'package:fury_core/src/excep/fury_exception.dart';
// import 'package:fury_core/src/memory/byte_reader.dart';
// import 'package:fury_core/src/memory/byte_writer.dart';
// import 'package:fury_core/src/ser/ser.dart';
//
// import '../../const/obj_type.dart';
//
// class CharSer extends Ser<String>{
//
//   const CharSer(): super(ObjType.UNKNOWN); // TODO: 貌似furyJava的char不是crossLang的，这里暂时用UNKNOWN
//
//   @override
//   String read(ByteReader br, FuryConfig conf) {
//     int value = br.readUint16();
//     return String.fromCharCode(value);
//   }
//
//   @override
//   void write(ByteWriter bd, String value) {
//     if (value.length != 1){
//       throw FuryException.serTypeIncomp(type, value);
//     }
//     bd.writeUint16(value.codeUnitAt(0));
//   }
// }