import 'package:fury/fury.dart';

part '../generated/enum_foo.g.dart';

@furyEnum
enum EnumFoo {
  A,
  B
}
@furyEnum
enum EnumSubClass {
  A,
  B;
}