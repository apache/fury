import 'dart:typed_data';

import 'package:fury_core/src/util/extension/uint8list_extensions.dart';
import 'package:fury_core/src/util/hash_util.dart';

import '../codec/meta_string_encoding.dart';

class MetaString {
  final String value;
  final MetaStrEncoding encoding;
  final int specialChar1;
  final int specialChar2;
  final Uint8List bytes;
  late final bool stripLastChar;

  int? _hash;
  
  MetaString(this.value, this.encoding, this.specialChar1, this.specialChar2, this.bytes){
    if (encoding != MetaStrEncoding.utf8){
      // if not utf8, then the bytes should not be empty
      assert(bytes.isNotEmpty);
      stripLastChar = (bytes[0] & 0x80) != 0;
    } else{
      stripLastChar = false;
    }
  }

  @override
  int get hashCode {
    // TODO: 模仿furyJava的hash写法，也许有更好的写法
    if (_hash == null){
      _hash = Object.hash(stripLastChar, encoding, specialChar1, specialChar2);
      _hash = 31 * _hash! + HashUtil.hashIntList(bytes);
    }
    return _hash!;
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
      (other is MetaString &&
        other.runtimeType == runtimeType &&
        other.encoding == encoding &&
        other.stripLastChar == stripLastChar &&
        other.specialChar1 == specialChar1 &&
        other.specialChar2 == specialChar2 &&
        bytes.memEquals(other.bytes)
      );
  }
}