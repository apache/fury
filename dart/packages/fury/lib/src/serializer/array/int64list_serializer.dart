import 'dart:typed_data';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/serializer/array/array_serializer.dart';
import 'package:fury/src/serializer/array/num_array_serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _Int64ListSerializerCache extends ArraySerializerCache{

  static Int64ListSerializer? _noRefSer;
  static Int64ListSerializer? _writeRefSer;

  const _Int64ListSerializerCache();

  @override
  Int64ListSerializer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Int64ListSerializer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Int64ListSerializer(false);
      return _noRefSer!;
    }
  }
}

final class Int64ListSerializer extends NumArraySerializer<int> {
  static const SerializerCache cache = _Int64ListSerializerCache();

  const Int64ListSerializer(bool writeRef) : super(ObjType.INT64_ARRAY, writeRef);

  @override
  TypedDataList<int> readToList(Uint8List copiedMem) {
    // need copy
    Int64List list = copiedMem.buffer.asInt64List();
    return list;
  }

  @override
  int get bytesPerNum => 8;
}