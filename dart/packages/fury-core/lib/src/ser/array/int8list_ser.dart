import 'dart:typed_data';
import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/ser/ser_cache.dart';
import 'array_ser.dart';
import 'num_array_ser.dart';

final class Int8ListSerCache extends ArraySerCache{

  static Int8ListSer? _noRefSer;
  static Int8ListSer? _writeRefSer;

  const Int8ListSerCache();

  @override
  Int8ListSer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Int8ListSer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Int8ListSer(false);
      return _noRefSer!;
    }
  }
}

final class Int8ListSer extends NumArraySer<int> {
  static const SerCache cache = Int8ListSerCache();

  const Int8ListSer(bool writeRef) : super(ObjType.INT8_ARRAY, writeRef);

  @override
  TypedDataList<int> readToList(Uint8List copiedMem) {
    // 这里需要做一步copy
    Int8List list = copiedMem.buffer.asInt8List();
    return list;
  }

  @override
  int get bytesPerNum => 1;
}