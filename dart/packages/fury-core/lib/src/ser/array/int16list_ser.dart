import 'dart:typed_data';
import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/ser/ser_cache.dart';
import 'array_ser.dart';
import 'num_array_ser.dart';

final class Int16ListSerCache extends ArraySerCache{

  static Int16ListSer? _noRefSer;
  static Int16ListSer? _writeRefSer;

  const Int16ListSerCache();

  @override
  Int16ListSer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Int16ListSer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Int16ListSer(false);
      return _noRefSer!;
    }
  }
}

final class Int16ListSer extends NumArraySer<int> {
  static const SerCache cache = Int16ListSerCache();

  const Int16ListSer(bool writeRef) : super(ObjType.INT16_ARRAY, writeRef);

  @override
  TypedDataList<int> readToList(Uint8List copiedMem) {
    // 这里需要做一步copy
    Int16List list = copiedMem.buffer.asInt16List();
    return list;
  }

  @override
  int get bytesPerNum => 2;
}