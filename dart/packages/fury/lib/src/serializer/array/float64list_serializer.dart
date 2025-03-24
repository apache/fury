import 'dart:typed_data';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/serializer/array/array_serializer.dart';
import 'package:fury/src/serializer/array/num_array_serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _Float64ListSerializerCache extends ArraySerializerCache{

  static Float64ListSerializer? _noRefSer;
  static Float64ListSerializer? _writeRefSer;

  const _Float64ListSerializerCache();

  @override
  Float64ListSerializer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Float64ListSerializer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Float64ListSerializer(false);
      return _noRefSer!;
    }
  }
}

final class Float64ListSerializer extends NumArraySerializer<double> {
  static const SerializerCache cache = _Float64ListSerializerCache();

  const Float64ListSerializer(bool writeRef) : super(ObjType.FLOAT64_ARRAY, writeRef);

  @override
  TypedDataList<double> readToList(Uint8List copiedMem) {
    // need copy
    Float64List list = copiedMem.buffer.asFloat64List();
    return list;
  }

  @override
  int get bytesPerNum => 8;
}