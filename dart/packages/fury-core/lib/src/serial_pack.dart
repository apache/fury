import 'package:fury_core/src/resolver/struct_hash_resolver.dart';

typedef GetTagByType = String Function(Type type);

abstract base class SerialPack {
  final StructHashResolver structHashResolver;
  final GetTagByType getTagByDartType;

  const SerialPack(
    this.structHashResolver,
    this.getTagByDartType,
  );
}