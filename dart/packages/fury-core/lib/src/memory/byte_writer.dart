import 'dart:typed_data';

import 'package:meta/meta.dart';

import 'fury_byte_writer.dart';

abstract base class ByteWriter{

  @protected
  final Endian endian = Endian.little;

  ByteWriter.internal();

  factory ByteWriter() => FuryByteWriter();

  // void setEndian(bool little){
  //   endian = little ? Endian.little : Endian.big;
  // }

  Uint8List toBytes();
  Uint8List takeBytes();

  void writeBool(bool v);
  void writeUint8(int byte);
  void writeUint16(int value);
  void writeUint32(int value);
  void writeUint64(int value);

  void writeInt8(int value);
  void writeInt16(int value);
  void writeInt32(int value);
  void writeInt64(int value);

  void writeFloat32(double value);
  void writeFloat64(double value);

  void writeBytes(List<int> bytes);

  void writeVarInt32(int v);
  void writeVarInt64(int v);
  void writeVarUint32Small7(int v);
  void writeVarUint36Small(int v);
  void writeVarUint32(int v);
  void writeVarUint64(int v);
}