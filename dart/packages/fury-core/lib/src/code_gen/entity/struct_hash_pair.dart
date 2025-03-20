import 'package:meta/meta.dart';

@immutable
class StructHashPair{
  final int fromFuryHash;
  final int toFuryHash;

  const StructHashPair(this.fromFuryHash, this.toFuryHash);
}