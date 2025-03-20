import 'dart:convert';
import 'dart:typed_data';

import 'package:fury_core/src/codec/meta_string_encoding.dart';
import 'package:fury_core/src/dev_annotation/optimize.dart';
import 'package:fury_core/src/meta/meta_string_byte.dart';

import '../meta_string_decoder.dart';

class FuryMetaStringDecoder extends MetaStringDecoder {

  const FuryMetaStringDecoder(super.specialChar1, super.specialChar2);

  // Decoding special char for LOWER_SPECIAL based on encoding mapping.
  @inline
  int _decodeLowerSpecialChar(int charValue) {
    if (charValue >= 0 && charValue <= 25) {
      // 'a' to 'z'
      return 97 + charValue;
    } else if (charValue == 26) {
      // '.'
      return 46;
    } else if (charValue == 27) {
      // '_'
      return 95;
    } else if (charValue == 28) {
      // '$'
      return 36;
    } else if (charValue == 29) {
      // '|'
      return 124;
    } else {
      throw ArgumentError('Unsupported character for LOWER_SPECIAL encoding: $charValue');
    }
  }

  // Decoding special char for LOWER_UPPER_DIGIT_SPECIAL based on encoding mapping.
  @inline
  int _decodeLowerUpperDigitSpecialChar(int charValue) {
    if (charValue >= 0 && charValue <= 25) {
      // 'a' to 'z'
      return 97 + charValue;
    } else if (charValue >= 26 && charValue <= 51) {
      // 'A' to 'Z'
      return 65 + (charValue - 26);
    } else if (charValue >= 52 && charValue <= 61) {
      // '0' to '9'
      return 48 + (charValue - 52);
    } else if (charValue == 62) {
      return specialChar1;
    } else if (charValue == 63) {
      return specialChar2;
    } else {
      throw ArgumentError('Unsupported character for LOWER_UPPER_DIGIT_SPECIAL encoding: $charValue');
    }
  }

  // TODO: 此方法因为较难理解，需要写一下仔细的文档，之后进行
  String _decodeLowerSpecial(Uint8List data){
    assert(data.isNotEmpty);
    var decoded = StringBuffer();
    int totalBits = data.length * 8;
    bool stripLastChar = (data[0] & 0x80) != 0; // the first bit of the first byte
    int bitMask = 0x1f; // 1 1111
    int bitIndex = 1;
    while (bitIndex + 5 <= totalBits && !(stripLastChar && (bitIndex + 2 * 5 > totalBits))){
      int byteIndex = bitIndex ~/ 8;
      int bitOffset = bitIndex % 8;
      int charValue = 0; // codeUnit
      // 01234567
      if (bitOffset > 3){
        // need to read from two bytes
        charValue =
        ((data[byteIndex] & 0xFF) << 8)
        | (byteIndex + 1 < data.length ? (data[byteIndex + 1] & 0xFF) : 0);
        charValue = ((charValue >> (11 - bitOffset)) & bitMask);
      }else {
        // read from one byte
        charValue = (data[byteIndex] >> (3 - bitOffset)) & bitMask;
      }
      bitIndex += 5;
      decoded.writeCharCode(_decodeLowerSpecialChar(charValue));
    }
    return decoded.toString();
  }

  String _decodeLowerUpperDigitSpecial(Uint8List data){
    StringBuffer buf = StringBuffer();
    int bitIndex = 1;
    bool stripLastChar = (data[0] & 0x80) != 0; // the first bit of the first byte
    int bitMask = 0x3f; // 0011 1111
    int numBits = data.length * 8;
    while (bitIndex + 6 <= numBits && !(stripLastChar && (bitIndex + 2 * 6 > numBits))){
      int byteIndex = bitIndex ~/ 8;
      int intraByteIndex = bitIndex % 8;
      int charValue = 0; // codeUnit
      // 01234567
      if (intraByteIndex > 2){
        // need to read from two bytes
        charValue = ((data[byteIndex] & 0xFF) << 8)
        | (byteIndex + 1 < data.length ? (data[byteIndex + 1] & 0xFF) : 0);
        charValue = (charValue >> (10 - intraByteIndex)) & bitMask;
      }else {
        // read from one byte
        charValue = data[byteIndex] >> (2 - intraByteIndex) & bitMask;
      }
      bitIndex += 6;
      buf.writeCharCode(_decodeLowerUpperDigitSpecialChar(charValue));
    }
    return buf.toString();
  }

  String _decodeRepFirstLowerSpecial(Uint8List data) {
    String decoded = _decodeLowerSpecial(data);
    Uint16List chars = Uint16List.fromList(decoded.codeUnits);
    chars[0] -= 32; // 'a' to 'A'
    return String.fromCharCodes(chars);
  }

  String _decodeRepAllToLowerSpecial(Uint8List data){
    String decoded = _decodeLowerSpecial(data);
    StringBuffer buf = StringBuffer();
    List<int> chars = decoded.codeUnits;
    int c;
    for (int i = 0; i< chars.length; ++i){
      if(chars[i] == 0x7c){
        c = chars[++i];
        buf.writeCharCode(c - 32); // 'A' to 'a'
      }else{
        buf.writeCharCode(chars[i]);
      }
    }
    return buf.toString();
  }


  @override
  String decode(Uint8List data, MetaStrEncoding encoding) {
    if (data.isEmpty) return '';
    switch (encoding) {
      case MetaStrEncoding.ls:
        return _decodeLowerSpecial(data);
      case MetaStrEncoding.luds:
        return _decodeLowerUpperDigitSpecial(data);
      case MetaStrEncoding.ftls:
        return _decodeRepFirstLowerSpecial(data);
      case MetaStrEncoding.atls:
        return _decodeRepAllToLowerSpecial(data);
      case MetaStrEncoding.utf8:
        return utf8.decode(data);
      // default:
      //   throw ArgumentError('Unsupported encoding: $encoding');
    }
  }

  @override
  @inline
  String decodeMetaString(MetaStringBytes data) {
    return decode(data.bytes, data.encoding);
  }

}