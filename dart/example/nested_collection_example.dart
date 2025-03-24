import 'package:fury/fury.dart';

part 'nested_collection_example.g.dart';

@furyClass
class NestedObject with _$NestedObjectFury{
  late final String name;
  late final List<String> names;
  late final Map<double, Map<String, int>> map;
}