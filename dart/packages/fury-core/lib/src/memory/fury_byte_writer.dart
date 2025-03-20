import 'dart:typed_data';

import 'package:fury_core/src/dev_annotation/optimize.dart';
import 'package:fury_core/src/memory/byte_writer.dart';

/// only serve one ser/deser process
final class FuryByteWriter extends ByteWriter{
  
  final BytesBuilder _buffer = BytesBuilder();
  final ByteData _tempByteData = ByteData(8); // 用于存放转换数据
  
  FuryByteWriter():super.internal();
  
  /// 获取当前缓冲区的大小
  int get length => _buffer.length;

  /// 获取最终的 `Uint8List`
  @override
  Uint8List toBytes() {
    return _buffer.toBytes();
  }

  @override
  Uint8List takeBytes() {
    return _buffer.takeBytes();
  }

  @inline
  @override
  void writeBool(bool v) {
    _buffer.addByte(v ? 1 : 0);
  }

  @inline
  /// 向缓冲区追加一个 `Uint8`
  @override
  void writeUint8(int value) {
    _buffer.addByte(value & 0xFF);
  }

  @inline
  /// 向缓冲区追加一个 `Uint16`（2 字节，Little Endian）
  @override
  void writeUint16(int value) {
    _tempByteData.setUint16(0, value, endian);
    _buffer.add(_tempByteData.buffer.asUint8List(0, 2));
  }

  @inline
  /// 向缓冲区追加一个 `Uint32`（4 字节，Little Endian）
  @override
  void writeUint32(int value) {
    _tempByteData.setUint32(0, value, endian);
    _buffer.add(_tempByteData.buffer.asUint8List(0, 4));
  }

  @inline
  /// 向缓冲区追加 `Uint64`（8 字节，Little Endian）
  @override
  void writeUint64(int value) {
    _tempByteData.setUint64(0, value, endian);
    _buffer.add(_tempByteData.buffer.asUint8List(0, 8));
  }

  @inline
  /// 向缓冲区追加一个 `Int8`
  @override
  void writeInt8(int value) {
    _tempByteData.setInt8(0, value);
    _buffer.add(_tempByteData.buffer.asUint8List(0, 1));
  }

  @inline
  @override
  void writeInt16(int value) {
    _tempByteData.setInt16(0, value, endian);
    _buffer.add(_tempByteData.buffer.asUint8List(0, 2));
  }

  @inline
  @override
  void writeInt32(int value,[int len = 4]) {
    _tempByteData.setInt32(0, value, endian);
    _buffer.add(_tempByteData.buffer.asUint8List(0, len));
  }

  @inline
  @override
  void writeInt64(int value,[int len = 8]) {
    _tempByteData.setInt64(0, value, endian);
    _buffer.add(_tempByteData.buffer.asUint8List(0, len));
  }


  /// 向缓冲区追加一个 `Float32`（4 字节，Little Endian）
  @inline
  @override
  void writeFloat32(double value) {
    _tempByteData.setFloat32(0, value, endian);
    _buffer.add(_tempByteData.buffer.asUint8List(0, 4));
  }

  /// 向缓冲区追加一个 `Float64`（8 字节，Little Endian）
  @inline
  @override
  void writeFloat64(double value) {
    _tempByteData.setFloat64(0, value, endian);
    _buffer.add(_tempByteData.buffer.asUint8List(0, 8));
  }

  @override
  @inline
  void writeBytes(List<int> bytes) {
    _buffer.add(bytes);
  }

  @override
  @inline
  void writeVarInt32(int v) {
    // 将符号位移到最右边, v向左移1位正式为了空出这个位置
    writeVarUint36Small((v<<1) ^ (v>>31));
  }

  @inline
  @override
  void writeVarInt64(int v) {
    writeVarUint64((v<<1) ^ (v>>63));
  }

  @inline
  @override
  void writeVarUint32(int v) {
    writeVarUint36Small(v); // 与FuryJava一致，也是先直接调用这个方法
  }

  @override
  void writeVarUint36Small(int v){
    int encoded = (v & 0x7f);
    if (v >>> 7 == 0) {
      _buffer.addByte(encoded);
      return;
    }
    encoded |= (((v & 0x3f80) << 1) | 0x80);
    if (v >>> 14 == 0) {
      writeInt32(encoded, 2);
      return;
    }
    return _continuePutVarInt36(encoded, v);
  }

  void _continuePutVarInt36(int encoded, int v){
    encoded |= (((v & 0x1fc000) << 2) | 0x8000);
    if (v >>> 21 ==0){
      writeInt32(encoded, 3);
      return;
    }
    encoded |= (((v & 0xfe00000) << 3) | 0x800000);
    if (v >>> 28 == 0){
      writeInt32(encoded, 4);
      return;
    }
    encoded |= (((v & 0xff0000000) << 4) | 0x80000000);
    writeInt64(encoded, 5);
  }

  void _continueWriteVarUint32Small7(int v){
    int encoded = (v & 0x7f);
    encoded |= (((v & 0x3f80) << 1) | 0x80);
    if (v >>> 14 == 0) {
      writeInt32(encoded, 2);
      return;
    }
    _continuePutVarInt36(encoded, v);
  }

  @override
  void writeVarUint32Small7(int v) {
    if (v >>> 7 ==0){
      _buffer.addByte(v);
      return;
    }
    _continueWriteVarUint32Small7(v);
  }

  @override
  void writeVarUint64(int v) {
    int varInt = (v & 0x7f);
    if(v >>> 7 == 0){
      _buffer.addByte(varInt);
      return;
    }
    varInt |= (((v & 0x3f80) << 1) | 0x80);
    if (v >>> 14 == 0) {
      writeInt32(varInt, 2);
      return;
    }
    varInt |= (((v & 0x1fc000) << 2) | 0x8000);
    if (v >>> 21 == 0) {
      writeInt32(varInt, 3);
      return;
    }
    varInt |= (((v & 0xfe00000) << 3) | 0x800000);
    if (v >>> 28 == 0) {
      writeInt32(varInt, 4);
      return;
    }
    varInt |= (((v & 0x7f0000000) << 4) | 0x80000000);
    if (v >>> 35 == 0) {
      writeInt64(varInt, 5);
      return;
    }
    varInt |= (((v & 0x3f800000000) << 5) | 0x8000000000);
    if (v >>> 42 == 0) {
      writeInt64(varInt, 6);
      return;
    }
    varInt |= (((v & 0x1fc0000000000) << 6) | 0x800000000000);
    if (v >>> 49 == 0) {
      writeInt64(varInt, 7);
      return;
    }
    varInt |= (((v & 0xfe000000000000) << 7) | 0x80000000000000);
    v >>>= 56;
    if (v == 0) {
      writeInt64(varInt, 8);
      return;
    }
    writeInt64(varInt | 0x8000000000000000);
    writeInt8(v & 0xff);
  }
}