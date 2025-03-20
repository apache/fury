import 'dart:io';
import 'dart:typed_data';

import 'package:fury_core/fury_core.dart';
import 'package:fury_test/entity/time_obj.dart';
final class TestFileUtil {
  static const String generatedFolder = '/test/generated_file/';

  static getWriteFile(String name, Uint8List bytes){
    String absolutePath = '${Directory.current.path}$generatedFolder$name';
    File file = File(absolutePath);
    if (!file.existsSync()) {
      file.createSync(recursive: true);
    }
    file.writeAsBytesSync(bytes);
    print('Data written to ${file.path}');
    return file;
  }
}

void mainNoUse() {
  Fury fury = Fury(
    xlangMode: true,
    refTracking: true,
  );
  TimeObj timeObj = TimeObj(
    LocalDate.epoch,
    LocalDate(2025, 3, 19),
    LocalDate(2023, 3, 20),
    LocalDate(2023, 1, 4),
    TimeStamp(0),
    TimeStamp(1714490301000000),
    TimeStamp(1742373844000000),
    TimeStamp(-1714490301000000),
  );

  fury.register($TimeObj, "test.TimeObj");
}