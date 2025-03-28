import 'package:fury/src/resolver/struct_hash_resolver.dart';

typedef GetTagByType = String Function(Type type);

abstract base class Pack {
  final StructHashResolver structHashResolver;
  final GetTagByType getTagByDartType;

  const Pack(
    this.structHashResolver,
    this.getTagByDartType,
  );
}