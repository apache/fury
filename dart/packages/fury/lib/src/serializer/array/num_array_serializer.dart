import 'dart:typed_data';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer/array/array_serializer.dart';
import 'package:fury/src/serializer_pack.dart';
import 'package:fury/src/util/math_checker.dart';

abstract base class NumArraySerializer<T extends num> extends ArraySerializer<T> {
  const NumArraySerializer(super.type, super.writeRef);

  TypedDataList<T> readToList(Uint8List copiedMem);

  int get bytesPerNum;

  @override
  TypedDataList<T> read(ByteReader br, int refId, DeserializerPack pack) {
    int num = br.readVarUint32Small7();
    return readToList(br.copyBytes(num));
  }

  @override
  void write(ByteWriter bw, covariant TypedDataList<T> v, SerPack pack) {
    if (!MathChecker.validInt32(v.lengthInBytes)){
      throw ArgumentError('NumArray lengthInBytes is not valid int32: ${v.lengthInBytes}');
    }
    bw.writeVarUint32(v.lengthInBytes);
    bw.writeBytes(v.buffer.asUint8List(v.offsetInBytes, v.lengthInBytes));
  }
}