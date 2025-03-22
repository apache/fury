import 'dart:typed_data';
import 'package:fury_core/src/util/math_checker.dart';

import '../../deser_pack.dart';
import '../../memory/byte_reader.dart';
import '../../memory/byte_writer.dart';
import '../../ser_pack.dart';
import 'array_ser.dart';

// abstract base class NumArraySerCache<T extends num> extends SerCache{
//
//   const NumArraySerCache();
//
//   @override
//   NumArraySer<T> getSer(FuryConfig conf,){
//     // 目前NumArray类型的Ser只有写入Ref和不写入Ref两种，所以这里只缓存两种
//     return getSerWithRef(conf.refTracking);
//   }
//   NumArraySer<T> getSerWithRef(bool writeRef);
// }


abstract base class NumArraySer<T extends num> extends ArraySer<T> {
  const NumArraySer(super.type, super.writeRef);

  TypedDataList<T> readToList(Uint8List copiedMem);

  int get bytesPerNum;

  @override
  TypedDataList<T> read(ByteReader br, int refId, DeserPack pack) {
    int num = br.readVarUint32Small7();
    return readToList(br.copyBytes(num));
  }

  @override
  void write(ByteWriter bw, covariant TypedDataList<T> v, SerPack pack) {
    if (!MathChecker.validInt32(v.lengthInBytes)){
      throw ArgumentError('NumArray lengthInBytes is not valid int32: ${v.lengthInBytes}');
    }
    bw.writeVarUint32(v.lengthInBytes);
    bw.writeBytes(v.buffer.asUint8List(v.offsetInBytes, v.lengthInBytes));
  }
}