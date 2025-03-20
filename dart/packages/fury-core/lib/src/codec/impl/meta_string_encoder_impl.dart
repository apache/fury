import 'dart:convert';
import 'dart:typed_data';

import 'package:fury_core/src/codec/meta_string_encoder.dart';
import 'package:fury_core/src/const/meta_string_const.dart';
import 'package:fury_core/src/codec/meta_string_encoding.dart';
import 'package:fury_core/src/meta/meta_string.dart';
import 'package:fury_core/src/util/string_util.dart';

import '../../util/char_util.dart';

final class FuryMetaStringEncoder extends MetaStringEncoder {

  const FuryMetaStringEncoder(super.specialChar1, super.specialChar2);

  int _charToValueLowerSpecial(int codeUint) {
    // 5 bits
    if (codeUint >= 97 && codeUint <= 122) {
      return codeUint - 97; // 'a' to 'z'
    } else if (codeUint == 46) {
      return 26; // '.'
    } else if (codeUint == 95) {
      return 27;  // '_'
    } else if (codeUint == 36) {
      return 28; // '$'
    } else if (codeUint == 124) {
      return 29; // '|'
    } else {
      throw ArgumentError('Unsupported character for LOWER_SPECIAL encoding:: ${String.fromCharCode(codeUint)}');
    }
  }

  // 写好暂缺的方法
  Uint8List _encodeLowerSpecial(String input){
    return _encodeGeneric(input.codeUnits, MetaStrEncoding.ls.bits);
  }

  Uint8List _encodeLowerUpperDigitSpecial(String input){
    return _encodeGeneric(input.codeUnits, MetaStrEncoding.luds.bits);
  }

  Uint8List _encodeFirstToLowerSpecial(String input){
    Uint16List chars = Uint16List.fromList(input.codeUnits);
    chars[0] += 32; // 'A' to 'a'
    return _encodeGeneric(chars, MetaStrEncoding.ftls.bits);
  }

  Uint8List _encodeRepAllToLowerSpecial(String input, int upperCount){
    Uint16List newChars = Uint16List(input.length + upperCount);
    int index = 0;
    for (var c in input.codeUnits){
      if (CharUtil.upper(c)){
        newChars[index++] = 0x7c; // '|'
        newChars[index++] = c + 32; // 'A' to 'a'
      } else {
        newChars[index++] = c;
      }
    }
    return _encodeGeneric(newChars, MetaStrEncoding.atls.bits);
  }

  int _charToValueLowerUpperDigitSpecial(int codeUnit) {
    // 6 bits
    if (codeUnit >= 97 && codeUnit <= 122) {
      // 'a' to 'z'
      return codeUnit - 97;
    } else if (codeUnit >= 65 && codeUnit <= 90) {
      // 'A' to 'Z'
      return codeUnit - 65 + 26;
    } else if (codeUnit >= 48 && codeUnit <= 57) {
      // '0' to '9'
      return codeUnit - 48 + 52;
    } else if (codeUnit == specialChar1) {
      // '.'
      return 62;
    } else if (codeUnit == specialChar2) {
      // '_'
      return 63;
    } else {
      throw ArgumentError('Unsupported character for LOWER_UPPER_DIGIT_SPECIAL encoding: ${String.fromCharCode(codeUnit)}');
    }
  }

  // TODO: (优化工作之后再考虑)如果使用int64进行原始填充，虽说byte不用更换太勤，但是实际上bitsPerChar不会那么大，导致charVal仍然会频繁变动，提升可能不大, 优化工作之后再考虑
  Uint8List _encodeGeneric(List<int> input, int bitsPerChar){
    assert(bitsPerChar >= 5 && bitsPerChar <= 32); // 根据官网的说明，bitsPerChar的范围最低是5
    int totalBits = input.length * bitsPerChar + 1;
    int byteLength = (totalBits + 7) ~/ 8;
    Uint8List bytes = Uint8List(byteLength);
    int byteInd = 0;
    int bitInd = 1; // Start from the second bit (the first is reserved for the flag)
    int charInd = 0;
    int charBitRemain = bitsPerChar; // Remaining bits to process for the current character
    int mask;
    while (charInd < input.length) {
      // bitsPerChar == 5 means LOWER_SPECIAL encoding, or LOWER_UPPER_DIGIT_SPECIAL encoding(only two)
      int charVal = (bitsPerChar == 5) ? _charToValueLowerSpecial(input[charInd]) : _charToValueLowerUpperDigitSpecial(input[charInd]);
      // Calculate how many bits are remaining in the current byte
      int nowByteRemain = 8 - bitInd;
      if (nowByteRemain >= charBitRemain) {
        // If the remaining bits in the current byte can fit the whole character value
        mask = (1 << charBitRemain) - 1; // Create a mask for the bits of the character
        bytes[byteInd] |= (charVal & mask) << (nowByteRemain - charBitRemain); // Place the character bits into the byte
        bitInd += charBitRemain;
        if (bitInd == 8) {
          // Move to the next byte if the current byte is filled
          ++byteInd;
          bitInd = 0;
        }
        // Character has been fully placed in the current byte, move to the next character
        ++charInd;
        charBitRemain = bitsPerChar; // Reset the remaining bits for the next character
      } else {
        // If the remaining bits in the current byte are not enough to hold the whole character
        mask = (1 << nowByteRemain) - 1; // Create a mask for the current available bits in the byte
        bytes[byteInd] |= (charVal >> (charBitRemain - nowByteRemain)) & mask; // Place part of the character bits into the byte
        ++byteInd; // Move to the next byte
        bitInd = 0; // Reset bit index for the new byte
        charBitRemain -= nowByteRemain; // Decrease the remaining bits for the character
      }
    }
    bool stripLastChar = bytes.length * 8 >= totalBits + bitsPerChar;
    if (stripLastChar) {
      // Mark the first byte as indicating a stripped character
      bytes[0] = (bytes[0] | 0x80);
    }
    return bytes;
  }

  MetaString _encode(String input, MetaStrEncoding encoding) {
    // TODO: 这里不进行检查input长度，此检查应该更早进行(写好后将此注释删除)
    assert(input.length < MetaStringConst.metaStrMaxLen);
    assert(encoding == MetaStrEncoding.utf8 || input.isNotEmpty); // 只有utf8编码可以为空
    if (input.isEmpty) return MetaString(input, encoding, specialChar1, specialChar2, Uint8List(0));
    if (encoding != MetaStrEncoding.utf8 && StringUtil.hasNonLatin(input)){
      throw ArgumentError('non-latin characters are not allowed in non-utf8 encoding');
    }
    late final Uint8List bytes;
    switch (encoding) {
      case MetaStrEncoding.ls:
        bytes = _encodeLowerSpecial(input);
        break;
      case MetaStrEncoding.luds:
        bytes = _encodeLowerUpperDigitSpecial(input);
        break;
      case MetaStrEncoding.ftls:
        bytes = _encodeFirstToLowerSpecial(input);
        break;
      case MetaStrEncoding.atls:
        final int upperCount = StringUtil.upperCount(input);
        bytes = _encodeRepAllToLowerSpecial(input, upperCount);
        break;
      case MetaStrEncoding.utf8:
        bytes = Uint8List.fromList(utf8.encode(input));
        break;
      // default:
      //   throw ArgumentError('Unsupported encoding: $encoding');
    }
    return MetaString(input, encoding, specialChar1, specialChar2, bytes);
  }

  @override
  MetaString encodeByAllowedEncodings(String input, List<MetaStrEncoding> encodings) {
    if (input.isEmpty) return MetaString(input, MetaStrEncoding.utf8, specialChar1, specialChar2, Uint8List(0));
    if (StringUtil.hasNonLatin(input)){
      return MetaString(
        input,
        MetaStrEncoding.utf8,
        specialChar1,
        specialChar2,
        utf8.encode(input),
      );
    }
    MetaStrEncoding encoding = decideEncoding(input, encodings);
    return _encode(input, encoding);
  }
}