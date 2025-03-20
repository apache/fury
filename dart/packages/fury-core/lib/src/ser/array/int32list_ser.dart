import 'dart:typed_data';
import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/ser/ser_cache.dart';
import 'array_ser.dart';
import 'num_array_ser.dart';

final class Int32ListSerCache extends ArraySerCache{

  static Int32ListSer? _noRefSer;
  static Int32ListSer? _writeRefSer;

  const Int32ListSerCache();

  @override
  Int32ListSer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Int32ListSer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Int32ListSer(false);
      return _noRefSer!;
    }
  }
}

final class Int32ListSer extends NumArraySer<int> {
  static const SerCache cache = Int32ListSerCache();

  const Int32ListSer(bool writeRef) : super(ObjType.INT32_ARRAY, writeRef);

  @override
  TypedDataList<int> readToList(Uint8List copiedMem) {
    // 这里需要做一步copy
    Int32List list = copiedMem.buffer.asInt32List();
    return list;
  }

  @override
  int get bytesPerNum => 4;
}