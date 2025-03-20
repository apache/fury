import 'dart:typed_data';

class MemoryUtil {
  static int getInt64LittleEndian(Uint8List buffer, int offset) {
    return buffer[offset] & 0xFF |
        (buffer[offset + 1] & 0xFF) << 8 |
        (buffer[offset + 2] & 0xFF) << 16 |
        (buffer[offset + 3] & 0xFF) << 24 |
        (buffer[offset + 4] & 0xFF) << 32 |
        (buffer[offset + 5] & 0xFF) << 40 |
        (buffer[offset + 6] & 0xFF) << 48 |
        (buffer[offset + 7] & 0xFF) << 56;
  }
}