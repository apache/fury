import 'dart:typed_data';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/serializer/array/array_serializer.dart';
import 'package:fury/src/serializer/array/num_array_serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _Float32ListSerializerCache extends ArraySerializerCache{

  static Float32ListSerializer? _noRefSer;
  static Float32ListSerializer? _writeRefSer;

  const _Float32ListSerializerCache();

  @override
  Float32ListSerializer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Float32ListSerializer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Float32ListSerializer(false);
      return _noRefSer!;
    }
  }
}

final class Float32ListSerializer extends NumArraySerializer<double> {
  static const SerializerCache cache = _Float32ListSerializerCache();

  const Float32ListSerializer(bool writeRef) : super(ObjType.FLOAT32_ARRAY, writeRef);

  @override
  TypedDataList<double> readToList(Uint8List copiedMem) {
    // need copy
    Float32List list = copiedMem.buffer.asFloat32List();
    return list;
  }
  @override
  int get bytesPerNum => 4;
}