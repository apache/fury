import 'package:fury_core/src/fury_data_type/fury_fixed_num.dart';
import 'package:fury_core/src/memory/byte_reader.dart';
import 'package:fury_core/src/memory/byte_writer.dart';
import 'package:fury_core/src/ser/primitive/primitive_ser_cache.dart';
import 'package:fury_core/src/ser/ser.dart';
import 'package:fury_core/src/fury_data_type/int8.dart';

import '../../deser_pack.dart';
import '../../const/obj_type.dart';
import '../../ser_pack.dart';
import '../ser_cache.dart';

final class _Int8SerCache extends PrimitiveSerCache{
  static Int8Ser? serRef;
  static Int8Ser? serNoRef;

  const _Int8SerCache();

  @override
  Ser getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Int8Ser._(true);
      return serRef!;
    } else {
      serNoRef ??= Int8Ser._(false);
      return serNoRef!;
    }
  }
}

// dart 无int8, 用户通过注解指定将dart中的int转换为int8, 故可能出现范围错误
final class Int8Ser extends Ser<FixedNum>{

  static const SerCache cache = _Int8SerCache();

  Int8Ser._(bool writeRef): super(ObjType.INT8, writeRef);

  @override
  Int8 read(ByteReader br, int refId, DeserPack pack) {
    return Int8(br.readInt8());// 使用有符号的8位整数，和FuryJava中的byte一致
  }

  @override
  void write(ByteWriter bw, covariant Int8 v, SerPack pack) {
    // if (value < -128 || value > 127){
    //   throw FuryException.serRangeExcep(objType, value);
    // }
    bw.writeInt8(v.toInt());
  }
}