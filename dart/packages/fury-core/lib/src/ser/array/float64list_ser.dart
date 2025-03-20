import 'dart:typed_data';
import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/ser/ser_cache.dart';
import 'array_ser.dart';
import 'num_array_ser.dart';

final class Float64ListSerCache extends ArraySerCache{

  static Float64ListSer? _noRefSer;
  static Float64ListSer? _writeRefSer;

  const Float64ListSerCache();

  @override
  Float64ListSer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Float64ListSer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Float64ListSer(false);
      return _noRefSer!;
    }
  }
}

final class Float64ListSer extends NumArraySer<double> {
  static const SerCache cache = Float64ListSerCache();

  const Float64ListSer(bool writeRef) : super(ObjType.FLOAT64_ARRAY, writeRef);

  @override
  TypedDataList<double> readToList(Uint8List copiedMem) {
    // 这里需要做一步copy
    Float64List list = copiedMem.buffer.asFloat64List();
    return list;
  }

  @override
  int get bytesPerNum => 8;
}