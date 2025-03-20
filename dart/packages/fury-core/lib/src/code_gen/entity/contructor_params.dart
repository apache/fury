import 'constructor_param.dart';

class ConstructorParams {
  // 注意按照分析顺序，positional参数中中optional聚集在list后面，与dart语法一致
  final List<ConstructorParam> positional;
  final List<ConstructorParam> named;

  const ConstructorParams(this.positional, this.named,);

  Iterable<ConstructorParam> get iterator sync* {
    for (var param in positional) {
      yield param;
    }
    for (var param in named) {
      yield param;
    }
  }
}