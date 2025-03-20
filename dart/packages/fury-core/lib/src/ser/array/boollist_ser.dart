import 'dart:typed_data';

import 'package:collection/collection.dart';
import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/ser/array/array_ser.dart';
import '../../deser_pack.dart';
import '../../memory/byte_reader.dart';
import '../../memory/byte_writer.dart';
import '../../ser_pack.dart';
import '../ser_cache.dart';

final class BoolListSerCache extends ArraySerCache {

  static BoolListSer? _noRefSer;
  static BoolListSer? _writeRefSer;

  const BoolListSerCache();

  @override
  BoolListSer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= BoolListSer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= BoolListSer(false);
      return _noRefSer!;
    }
  }
}

final class BoolListSer extends ArraySer<bool>{

  static const SerCache cache = BoolListSerCache();

  const BoolListSer(bool writeRef) : super(ObjType.BOOL_ARRAY, writeRef);

  @override
  BoolList read(ByteReader br, int refId, DeserPack pack){
    int num = br.readVarUint32Small7();
    BoolList list = BoolList(num);
    Uint8List bytes = br.readBytesView(num);
    for (int i = 0; i < num; ++i) {
      list[i] = bytes[i] != 0;
    }
    return list;
  }

  @override
  void write(ByteWriter bw, covariant BoolList v, SerPack pack) {
    bw.writeVarUint32(v.length);
    Uint8List bytes = Uint8List(v.length);
    for (int i = 0; i < v.length; ++i) {
      bytes[i] = v[i] ? 1 : 0;
    }
    bw.writeBytes(bytes);
  }
}