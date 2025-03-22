import 'dart:convert';
import 'dart:typed_data';

import 'package:fury_core/src/dev_annotation/optimize.dart';

class StringUtil{

  static final RegExp _nonLatinRegex = RegExp(r'[^\x00-\xFF]');

  @inline
  static bool hasNonLatin(String input) {
    return _nonLatinRegex.hasMatch(input);
  }

  static int upperCount(String input) {
    int count = 0;
    for (final codeUnit in input.codeUnits) {
      if (codeUnit >= 0x41 && codeUnit <= 0x5A) {
        ++count;
      }
    }
    return count;
  }

  static String addingTypeNameAndNs(String ns, String tn) {
    if (ns.isEmpty) return tn;
    StringBuffer sb = StringBuffer();
    sb.write(ns);
    sb.write('.');
    sb.write(tn);
    return sb.toString();
  }

  static int computeUtf8StringHash(String str){
    int hash = 17;
    Uint8List utf8Bytes = utf8.encode(str);
    for (int byte in utf8Bytes){
      hash = hash * 31 + byte;
      while (hash > 0x7FFFFFFF){
        hash ~/= 7;
      }
    }
    return hash;
  }

  /// 向量化的方法竟然没有正则快，先不用
  @Deprecated('perf reason')
  bool isLatin5(String input) {
    final codeUnits = input.codeUnits;
    if (codeUnits.isEmpty) return true;
    // 获取当前平台的字节序
    Endian endian = Endian.little;

    // 根据字节序调整掩码
    const highByteMaskLittle = 0xFF00FF00FF00FF00; // 小端序掩码
    const highByteMaskBig = 0x00FF00FF00FF00FF;    // 大端序掩码
    final highByteMask = endian == Endian.little ? highByteMaskLittle : highByteMaskBig;

    // 将 codeUnits 转换为字节数据（直接共享内存）
    final buffer = Uint16List.fromList(codeUnits).buffer;
    final byteData = ByteData.view(buffer);
    final totalBytes = codeUnits.length * 2;

    // 批量处理 8 字节（4 字符）
    final batchSize = 8;
    final batchCount = totalBytes ~/ batchSize;
    final uint64List = byteData.buffer.asUint64List();

    for (int i = 0; i < batchCount; i++) {
      final value = uint64List[i];
      if (value & highByteMask != 0) return false;
    }

    // 处理剩余字节（按正确字节序读取）
    final remainderOffset = batchCount * batchSize;
    for (int offset = remainderOffset; offset < totalBytes; offset += 2) {
      final codeUnit = byteData.getUint16(offset, endian);
      if (codeUnit > 0xFF) return false;
    }
    return true;
  }
}