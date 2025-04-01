/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import 'dart:io';
import 'dart:typed_data';
import 'package:checks/checks.dart';
import 'package:fury/fury.dart';
import 'package:fury_test/util/test_file_util.dart';
import 'package:fury_test/util/test_process_util.dart';

final class CrossLangUtil{
  static const String pythonExecutable = "C:/Users/86511/.conda/envs/pyfury_dev6/python.exe";
  static const String pythonMudule= "pyfury.tests.test_cross_language";
  static const Map<String,String> env = {'ENABLE_CROSS_LANGUAGE_TESTS': 'true'};

  static bool executeWithPython(String testName, String filePath, [int waitingSec = 30]){
    List<String> command = [
      pythonExecutable,
      "-m",
      pythonMudule,
      testName,
      filePath
    ];
    return TestProcessUtil.executeCommandSync(command, waitingSec, env);
  }

  static void structRoundBack(Fury fury, Object? obj, String testName) {
    Uint8List bytes = fury.toFury(obj);
    Object? obj2 = fury.fromFury(bytes);
    check(obj2).equals(obj);
    // get current working directory
    File file = TestFileUtil.getWriteFile(testName, bytes);
    try{
      bool exeRes = CrossLangUtil.executeWithPython(testName, file.path);
      check(exeRes).isTrue();
      Object? deObj = fury.fromFury(file.readAsBytesSync());
      check(deObj).equals(obj);
    }finally{
      file.deleteSync();
    }
  }

  static T serDe<T> (Fury fury1, Fury fury2, T obj) {
    Uint8List bytes = fury1.toFury(obj);
    Object? obj2 = fury2.fromFury(bytes);
    return obj2 as T;
  }
}