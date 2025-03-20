import 'package:fury_core/src/memory/byte_reader.dart';
import 'package:fury_core/src/memory/byte_writer.dart';
import 'package:fury_core/src/ser/primitive/primitive_ser_cache.dart';
import 'package:fury_core/src/ser/ser.dart';
import 'package:fury_core/src/ser/ser_cache.dart';

import '../../deser_pack.dart';
import '../../const/obj_type.dart';
import '../../ser_pack.dart';

final class _Int64SerCache extends PrimitiveSerCache{
  static Int64Ser? serRef;
  static Int64Ser? serNoRef;

  const _Int64SerCache();

  @override
  Ser getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Int64Ser._(true);
      return serRef!;
    } else {
      serNoRef ??= Int64Ser._(false);
      return serNoRef!;
    }
  }
}

final class Int64Ser extends Ser<int> {

  static const SerCache cache = _Int64SerCache();

  Int64Ser._(bool writeRef): super(ObjType.INT64, writeRef);

  @override
  int read(ByteReader br, int refId, DeserPack pack) {
    return br.readVarInt64();
  }

  @override
  void write(ByteWriter bw, int v, SerPack pack) {
    bw.writeVarInt64(v);
  }
}