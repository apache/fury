import 'dart:typed_data';
import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/ser/ser_cache.dart';
import 'array_ser.dart';
import 'num_array_ser.dart';

final class Int64ListSerCache extends ArraySerCache{

  static Int64ListSer? _noRefSer;
  static Int64ListSer? _writeRefSer;

  const Int64ListSerCache();

  @override
  Int64ListSer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Int64ListSer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Int64ListSer(false);
      return _noRefSer!;
    }
  }
}

final class Int64ListSer extends NumArraySer<int> {
  static const SerCache cache = Int64ListSerCache();

  const Int64ListSer(bool writeRef) : super(ObjType.INT64_ARRAY, writeRef);

  @override
  TypedDataList<int> readToList(Uint8List copiedMem) {
    // 这里需要做一步copy
    Int64List list = copiedMem.buffer.asInt64List();
    return list;
  }

  @override
  int get bytesPerNum => 8;
}