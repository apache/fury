import 'dart:typed_data';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/dev_annotation/optimize.dart';
import 'package:fury/src/serializer/array/array_serializer.dart';
import 'package:fury/src/serializer/array/num_array_serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _Uint8ListSerializerCache extends ArraySerializerCache {

  static Uint8ListSerializer? _noRefSer;
  static Uint8ListSerializer? _writeRefSer;

  const _Uint8ListSerializerCache();

  @override
  Uint8ListSerializer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Uint8ListSerializer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Uint8ListSerializer(false);
      return _noRefSer!;
    }
  }
}

final class Uint8ListSerializer extends NumArraySerializer<int> {
  static const SerializerCache cache = _Uint8ListSerializerCache();

  const Uint8ListSerializer(bool writeRef) : super(ObjType.BINARY, writeRef);

  @inline
  @override
  TypedDataList<int> readToList(Uint8List copiedMem) {
    // need copy
    return copiedMem;
  }

  @override
  int get bytesPerNum => 1;
}