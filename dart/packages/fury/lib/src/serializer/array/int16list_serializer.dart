import 'dart:typed_data';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/serializer/array/array_serializer.dart';
import 'package:fury/src/serializer/array/num_array_serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

final class _Int16ListSerializerCache extends ArraySerializerCache{

  static Int16ListSerializer? _noRefSer;
  static Int16ListSerializer? _writeRefSer;

  const _Int16ListSerializerCache();

  @override
  Int16ListSerializer getSerWithRef(bool writeRef) {
    if (writeRef) {
      _writeRefSer ??= Int16ListSerializer(true);
      return _writeRefSer!;
    } else {
      _noRefSer ??= Int16ListSerializer(false);
      return _noRefSer!;
    }
  }
}

final class Int16ListSerializer extends NumArraySerializer<int> {
  static const SerializerCache cache = _Int16ListSerializerCache();

  const Int16ListSerializer(bool writeRef) : super(ObjType.INT16_ARRAY, writeRef);

  @override
  TypedDataList<int> readToList(Uint8List copiedMem) {
    // need copy
    Int16List list = copiedMem.buffer.asInt16List();
    return list;
  }

  @override
  int get bytesPerNum => 2;
}