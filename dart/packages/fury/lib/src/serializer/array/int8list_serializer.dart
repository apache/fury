import 'dart:typed_data';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/serializer/array/array_serializer.dart';
import 'package:fury/src/serializer/array/num_array_serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _Int8ListSerializerCache extends ArraySerializerCache{

  static Int8ListSerializer? _noRefSer;
  static Int8ListSerializer? _writeRefSer;

  const _Int8ListSerializerCache();

  @override
  Int8ListSerializer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Int8ListSerializer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Int8ListSerializer(false);
      return _noRefSer!;
    }
  }
}

final class Int8ListSerializer extends NumArraySerializer<int> {
  static const SerializerCache cache = _Int8ListSerializerCache();

  const Int8ListSerializer(bool writeRef) : super(ObjType.INT8_ARRAY, writeRef);

  @override
  TypedDataList<int> readToList(Uint8List copiedMem) {
    // need copy
    Int8List list = copiedMem.buffer.asInt8List();
    return list;
  }

  @override
  int get bytesPerNum => 1;
}