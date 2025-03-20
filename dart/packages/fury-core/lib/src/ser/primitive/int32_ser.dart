import 'package:fury_core/src/fury_data_type/fury_fixed_num.dart';
import 'package:fury_core/src/memory/byte_reader.dart';
import 'package:fury_core/src/memory/byte_writer.dart';
import 'package:fury_core/src/ser/primitive/primitive_ser_cache.dart';
import 'package:fury_core/src/ser/ser.dart';
import 'package:fury_core/src/fury_data_type/int32.dart';

import '../../deser_pack.dart';
import '../../const/obj_type.dart';
import '../../ser_pack.dart';
import '../ser_cache.dart';

final class _Int32SerCache extends PrimitiveSerCache{
  static Int32Ser? serRef;
  static Int32Ser? serNoRef;

  const _Int32SerCache();

  @override
  Ser getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Int32Ser._(true);
      return serRef!;
    } else {
      serNoRef ??= Int32Ser._(false);
      return serNoRef!;
    }
  }
}

// dart 无int32, 用户通过注解指定将dart中的int转换为int32, 故可能出现范围错误
final class Int32Ser extends Ser<FixedNum>{
  static const SerCache cache = _Int32SerCache();

  Int32Ser._(bool writeRef): super(ObjType.INT32, writeRef);

  @override
  Int32 read(ByteReader br, int refId, DeserPack pack) {
    int res = br.readVarInt32();
    return Int32(res);
  }

  @override
  void write(ByteWriter bw, covariant Int32 v, SerPack pack) {
    // 用户自己承诺的使用int32, 这里不做检查直接
    // if (value < -2147483648 || value > 2147483647){
    //   throw FuryException.serRangeExcep(objType, value);
    // }
    bw.writeVarInt32(v.toInt());
  }
}