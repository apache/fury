import 'package:fury_core/src/fury_data_type/float32.dart';
import 'package:fury_core/src/fury_data_type/fury_fixed_num.dart';
import 'package:fury_core/src/memory/byte_reader.dart';
import 'package:fury_core/src/memory/byte_writer.dart';
import 'package:fury_core/src/ser/primitive/primitive_ser_cache.dart';
import 'package:fury_core/src/ser/ser.dart';

import '../../deser_pack.dart';
import '../../const/obj_type.dart';
import '../../ser_pack.dart';
import '../ser_cache.dart';

final class _Float32SerCache extends PrimitiveSerCache{
  static Float32Ser? serRef;
  static Float32Ser? serNoRef;

  const _Float32SerCache();

  @override
  Ser getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Float32Ser._(true);
      return serRef!;
    } else {
      serNoRef ??= Float32Ser._(false);
      return serNoRef!;
    }
  }
}


// dart 无float32, 用户通过注解指定将dart中的double转换为float32, 故可能出现精度错误
final class Float32Ser extends Ser<FixedNum>{

  static const SerCache cache = _Float32SerCache();

  Float32Ser._(bool writeRef): super(ObjType.FLOAT32, writeRef);

  @override
  Float32 read(ByteReader br, int refId, DeserPack pack) {
    return Float32(br.readFloat32());
  }

  @override
  void write(ByteWriter bw, covariant Float32 v, SerPack pack) {
    // 这里不做检查
    // if (value.isInfinite || value.isNaN || value < -3.4028235e38 || value > 3.4028235e38){
    //   throw FuryException.serRangeExcep(objType, value);
    // }
    bw.writeFloat32(v.toDouble());
  }
}