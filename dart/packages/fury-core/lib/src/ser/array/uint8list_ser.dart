import 'dart:typed_data';
import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/dev_annotation/optimize.dart';
import '../ser_cache.dart';
import 'array_ser.dart';
import 'num_array_ser.dart';

final class Uint8ListSerCache extends ArraySerCache {

  static Uint8ListSer? _noRefSer;
  static Uint8ListSer? _writeRefSer;

  const Uint8ListSerCache();

  @override
  Uint8ListSer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Uint8ListSer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Uint8ListSer(false);
      return _noRefSer!;
    }
  }
}

final class Uint8ListSer extends NumArraySer<int> {
  static const SerCache cache = Uint8ListSerCache();

  const Uint8ListSer(bool writeRef) : super(ObjType.BINARY, writeRef);

  @inline
  @override
  TypedDataList<int> readToList(Uint8List copiedMem) {
    // 这里需要做一步copy
    return copiedMem;
  }

  @override
  int get bytesPerNum => 1;
}