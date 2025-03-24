import 'package:meta/meta.dart';

@immutable
class Either<L, R> {
  final L? left;
  final R? right;

  Either.left(this.left) : right = null;
  Either.right(this.right) : left = null;

  bool get isLeft => left != null;
  bool get isRight => right != null;
}