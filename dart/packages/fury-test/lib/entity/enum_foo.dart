import 'package:fury_core/fury_core.dart';

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