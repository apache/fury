import 'dart:typed_data';

import 'package:fury_core/src/memory/fury_byte_reader.dart';
import 'package:meta/meta.dart';

/// An abstract class for reading bytes from a stream.
abstract base class ByteReader {

  @protected
  final Endian endian = Endian.little;

  ByteReader.internal();

  factory ByteReader.forBytes(Uint8List data, {int offset = 0, int? length})
    => FuryByteReader(data, offset: offset, length: length);

  // void setEndian(bool little){
  //   endian = little ? Endian.little : Endian.big;
  // }

  void skip(int length);

  bool readBool();

  /// Reads an unsigned 8-bit integer from the stream.
  int readUint8();

  /// Reads an unsigned 16-bit integer from the stream.
  int readUint16();

  /// Reads an unsigned 32-bit integer from the stream.
  int readUint32();

  /// Reads an unsigned 64-bit integer from the stream.
  int readUint64();

  /// Reads a signed 8-bit integer from the stream.
  int readInt8();

  /// Reads a signed 16-bit integer from the stream.
  int readInt16();

  /// Reads a signed 32-bit integer from the stream.
  int readInt32();

  /// Reads a signed 64-bit integer from the stream.
  int readInt64();

  /// Reads a 32-bit floating point number from the stream.
  double readFloat32();

  /// Reads a 64-bit floating point number from the stream.
  double readFloat64();

  int readVarUint36Small();

  int readVarInt32();

  int readVarUint32();

  int readVarInt64();

  // int readVarUint36Slow();
  // int readVarUint32Slow();
  // int readVarUint64Slow();

  int readVarUint32Small7();

  int readVarUint32Small14();

  /// read [length] bytes as int64
  int readBytesAsInt64(int length);

  Uint8List readBytesView(int length);
  
  Uint8List copyBytes(int length);

  // MemRange readBytesViewRange(int length);

  Uint16List readCopyUint16List(int byteNum);
}