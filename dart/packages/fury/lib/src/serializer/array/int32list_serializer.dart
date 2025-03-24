import 'dart:typed_data';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/serializer/array/array_serializer.dart';
import 'package:fury/src/serializer/array/num_array_serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _Int32ListSerializerCache extends ArraySerializerCache{

  static Int32ListSerializer? _noRefSer;
  static Int32ListSerializer? _writeRefSer;

  const _Int32ListSerializerCache();

  @override
  Int32ListSerializer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Int32ListSerializer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Int32ListSerializer(false);
      return _noRefSer!;
    }
  }
}

final class Int32ListSerializer extends NumArraySerializer<int> {
  static const SerializerCache cache = _Int32ListSerializerCache();

  const Int32ListSerializer(bool writeRef) : super(ObjType.INT32_ARRAY, writeRef);

  @override
  TypedDataList<int> readToList(Uint8List copiedMem) {
    // need copy
    Int32List list = copiedMem.buffer.asInt32List();
    return list;
  }

  @override
  int get bytesPerNum => 4;
}