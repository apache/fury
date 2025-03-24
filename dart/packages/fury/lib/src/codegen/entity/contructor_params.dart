import 'constructor_param.dart';

class ConstructorParams {
  // Note that according to the analysis order, optional parameters in positional
  // are gathered at the end of the list, consistent with Dart syntax
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