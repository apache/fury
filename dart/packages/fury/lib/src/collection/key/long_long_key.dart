class LongLongKey{
  final int _first;
  final int _second;

  int? _hashCode;

  LongLongKey(this._first, this._second);

  @override
  int get hashCode{
    // TODO: Maybe we can use other hash function that is faster
    _hashCode ??= Object.hash(_first, _second);
    return _hashCode!;
  }

  @override
  bool operator ==(Object other) {
    return identical(this, other) ||
        other is LongLongKey &&
        runtimeType == other.runtimeType &&
        _first == other._first &&
        _second == other._second;
  }
}