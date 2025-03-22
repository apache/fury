import 'package:fury_core/src/memory/byte_reader.dart';
import 'package:fury_core/src/memory/byte_writer.dart';
import 'package:fury_core/src/ser/primitive/primitive_ser_cache.dart';
import 'package:fury_core/src/ser/ser.dart';

import '../../deser_pack.dart';
import '../../const/obj_type.dart';
import '../../ser_pack.dart';
import '../ser_cache.dart';

final class _Float64SerCache extends PrimitiveSerCache{
  static Float64Ser? serRef;
  static Float64Ser? serNoRef;

  const _Float64SerCache();

  @override
  Ser getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Float64Ser._(true);
      return serRef!;
    } else {
      serNoRef ??= Float64Ser._(false);
      return serNoRef!;
    }
  }
}

final class Float64Ser extends Ser<double>{

  static const SerCache cache = _Float64SerCache();

  Float64Ser._(bool writeRef): super(ObjType.FLOAT64, writeRef);

  @override
  double read(ByteReader br, int refId, DeserPack pack) {
    return br.readFloat64();
  }

  @override
  void write(ByteWriter bw, double v, SerPack pack) {
    bw.writeFloat64(v);
  }

}