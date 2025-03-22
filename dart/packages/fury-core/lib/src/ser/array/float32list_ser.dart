import 'dart:typed_data';
import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/ser/ser_cache.dart';
import 'array_ser.dart';
import 'num_array_ser.dart';

final class Float32ListSerCache extends ArraySerCache{

  static Float32ListSer? _noRefSer;
  static Float32ListSer? _writeRefSer;

  const Float32ListSerCache();

  @override
  Float32ListSer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Float32ListSer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Float32ListSer(false);
      return _noRefSer!;
    }
  }
}

final class Float32ListSer extends NumArraySer<double> {
  static const SerCache cache = Float32ListSerCache();

  const Float32ListSer(bool writeRef) : super(ObjType.FLOAT32_ARRAY, writeRef);

  @override
  TypedDataList<double> readToList(Uint8List copiedMem) {
    // 这里需要做一步copy
    Float32List list = copiedMem.buffer.asFloat32List();
    return list;
  }
  @override
  int get bytesPerNum => 4;
}