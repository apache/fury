import 'package:fury/fury.dart';

part '../generated/some_class.g.dart';

@furyClass
class SomeClass with _$SomeClassFury {
  late int id;
  late String name;
  late Map<String, double> map;

  SomeClass(this.id, this.name, this.map);

  SomeClass.noArgs();
}