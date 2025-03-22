import 'package:fury_core/src/fury_data_type/fury_fixed_num.dart';
import 'package:fury_core/src/memory/byte_reader.dart';
import 'package:fury_core/src/memory/byte_writer.dart';
import 'package:fury_core/src/ser/primitive/primitive_ser_cache.dart';
import 'package:fury_core/src/ser/ser.dart';
import 'package:fury_core/src/fury_data_type/int16.dart';

import '../../deser_pack.dart';
import '../../const/obj_type.dart';
import '../../ser_pack.dart';
import '../ser_cache.dart';

final class _Int16SerCache extends PrimitiveSerCache{
  static Int16Ser? serRef;
  static Int16Ser? serNoRef;

  const _Int16SerCache();

  @override
  Ser getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Int16Ser._(true);
      return serRef!;
    } else {
      serNoRef ??= Int16Ser._(false);
      return serNoRef!;
    }
  }
}

// dart 无int16, 用户通过注解指定将dart中的int转换为int16, 故可能出现范围错误
final class Int16Ser extends Ser<FixedNum>{

  static const SerCache cache = _Int16SerCache();

  Int16Ser._(bool writeRef): super(ObjType.INT16, writeRef);

  @override
  Int16 read(ByteReader br, int refId, DeserPack pack) {
    return Int16(br.readInt16());
  }

  @override
  void write(ByteWriter bw, covariant Int16 v, SerPack pack) {
    // if (value < -32768 || value > 32767){
    //   throw FuryException.serRangeExcep(objType, value);
    // }
    bw.writeInt16(v.toInt());
  }
}