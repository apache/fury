import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/memory/byte_reader.dart';
import 'package:fury_core/src/memory/byte_writer.dart';
import 'package:fury_core/src/ser/primitive/primitive_ser_cache.dart';
import 'package:fury_core/src/ser/ser.dart';

import '../../deser_pack.dart';
import '../../ser_pack.dart';
import '../ser_cache.dart';

final class _BoolSerCache extends PrimitiveSerCache{
  static BoolSer? serRef;
  static BoolSer? serNoRef;

  const _BoolSerCache();

  @override
  Ser getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= BoolSer._(true);
      return serRef!;
    } else {
      serNoRef ??= BoolSer._(false);
      return serNoRef!;
    }
  }
}


final class BoolSer extends Ser<bool> {

  static const SerCache cache = _BoolSerCache();

  BoolSer._(bool writeRef): super(ObjType.BOOL, writeRef);

  @override
  bool read(ByteReader br, int refId, DeserPack pack) {
    return br.readUint8() != 0;
  }

  @override
  void write(ByteWriter bw, bool v, SerPack pack) {
    bw.writeBool(v);
  }
}