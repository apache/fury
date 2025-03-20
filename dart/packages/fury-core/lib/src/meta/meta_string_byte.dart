import 'dart:typed_data';

import 'package:fury_core/src/codec/meta_string_encoding.dart';
import 'package:fury_core/src/meta/meta_string.dart';
import 'package:fury_core/src/util/extension/uint8list_extensions.dart';
import 'package:fury_core/src/util/murmur3hash.dart';

final class MetaStringBytes{

  // static late final MetaStringBytes empty;

  // static const int defDynamicWriteStrId = -1;
  static const int _headerMask = 0xff;
  // bytes maybe a view of a larger buffer, do not modify it
  final Uint8List _bytes;
  final int _hashCode;
  late final MetaStrEncoding _encoding;
  late final int _first8Bytes;
  late final int _second8Bytes;
  // int dynamicWriteStrId = defDynamicWriteStrId;

  // cons
  MetaStringBytes(this._bytes, this._hashCode){
    assert(hashCode != 0);
    int header = hashCode & _headerMask;
    _encoding = MetaStrEncoding.fromId(header);
    // in case length is less than 16
    Uint8List data = _bytes;
    if (_bytes.length < 16){
      data = Uint8List(16);
      data.setAll(0, _bytes);
    }
    ByteData byteData = data.buffer.asByteData();
    _first8Bytes = byteData.getInt64(0, Endian.little);
    _second8Bytes = byteData.getInt64(8, Endian.little);
  }

  factory MetaStringBytes.of(MetaString ms){
    Uint8List bytes = ms.bytes;
    int hashCode = Murmur3Hash.hash128x64(bytes, 0, bytes.length, 47).$1;
    hashCode = hashCode.abs();
    if (hashCode == 0){
      hashCode = 256;
    }
    hashCode &= 0xffffffffffffff00;
    MetaStrEncoding encoding = ms.encoding;
    int header = encoding.id & _headerMask;
    hashCode = hashCode | header;
    return MetaStringBytes(bytes, hashCode);
  }

  // getter
  Uint8List get bytes => _bytes;
  int get length => _bytes.length;
  MetaStrEncoding get encoding => _encoding;
  // bool get isDefWriteId => dynamicWriteStrId == defDynamicWriteStrId;

  @override
  bool operator == (Object other) {
    return identical(this, other) ||
        other is MetaStringBytes &&
            runtimeType == other.runtimeType &&
            _first8Bytes == other._first8Bytes &&
            _second8Bytes == other._second8Bytes &&
            _encoding == other._encoding &&
            _bytes.memEquals(other._bytes);
  }

  @override
  int get hashCode => _hashCode;

  // @inline
  // void resetDynamicWriteStrId(){
  //   dynamicWriteStrId = defDynamicWriteStrId;
  // }
}