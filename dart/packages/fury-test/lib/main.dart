import 'dart:convert';
import 'dart:typed_data';

import 'package:fury/fury.dart';

import 'entity/complex_obj_4.dart';

void main(){
  ComplexObject4 obj = ComplexObject4();
  obj.f1 = 'SSSSSSSSSSSSSSSS';
  obj.f2 = "abc";
  obj.f3 = List.generate(100, (index) => index)
      .map((e) => e.toString())
      .toList()
      .cast<String>();
  obj.f4 = List.generate(100, (index) => index)
      .asMap()
      .map((k, v) => MapEntry(k, v.toDouble()));
  obj.f5 = 127;
  obj.f6 = 65535;
  obj.f7 = 123;
  obj.f8 = 0x7FFFFFFFFFFFFFFF;
  obj.f9 = 0.5;
  obj.f10 = 1 / 3.0;
  obj.f11 = List.generate(100, (index) => index)
      .map((e) => (e * 1.0))
      .toList()
      .cast<double>();
  obj.f12 = List.generate(100, (index) => index)
      .map((e) => e.toInt())
      .toList();

  Stopwatch sw = Stopwatch()..start();
  for(int i = 0; i < 100000; ++i){
    String str = jsonEncode(obj);
  }
  sw.stop();
  print('toJson: ${sw.elapsedMilliseconds} ms');

  Fury fury = Fury(refTracking: true);
  fury.register($ComplexObject4);
  sw = Stopwatch()..start();
  for(int i = 0; i < 100000; ++i){
    fury.toFury(obj);
  }
  sw.stop();
  print('fury.toFury: ${sw.elapsedMilliseconds} ms');

  // test deserialization
  sw = Stopwatch()..start();
  String str = jsonEncode(obj);
  for(int i = 0; i < 100000; ++i){
    Map<String, dynamic> map = jsonDecode(str);
    ComplexObject4 obj2 = ComplexObject4.fromJson(map);
  }
  sw.stop();
  print('fromJson: ${sw.elapsedMilliseconds} ms');

  sw = Stopwatch()..start();
  Uint8List bytes = fury.toFury(obj);
  for(int i = 0; i < 100000; ++i){
    ComplexObject4 obj2 = fury.fromFury(bytes) as ComplexObject4;
  }

  sw.stop();
  print('fury.fromFury: ${sw.elapsedMilliseconds} ms');

}