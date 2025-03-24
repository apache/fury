import 'dart:io';
import 'dart:typed_data';
import 'package:path/path.dart' as path;

final class TestFileUtil {
  static const String generatedFolder = 'test/generated_file/';

  static getWriteFile(String name, Uint8List bytes) {
    String absolutePath = path.join(Directory.current.path, generatedFolder, name);
    File file = File(absolutePath);
    if (!file.existsSync()) {
      file.createSync(recursive: true);
    }
    file.writeAsBytesSync(bytes);
    print('Data written to ${file.path}');
    return file;
  }
}