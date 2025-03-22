// 目前没有用到
class LibStringKey{
  final String scheme; // 貌似只有dart和package这两种
  final String path;

  int? _hashCode;

  LibStringKey(this.scheme, this.path);

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
          other is LibStringKey &&
              runtimeType == other.runtimeType &&
              scheme == other.scheme &&
              path == other.path;

  @override
  int get hashCode {
    if (_hashCode != null) return _hashCode!;

    int len1 = path.length;
    int letter1 = scheme.isNotEmpty ? scheme.codeUnitAt(0) : 32;

    if (len1 == 0) {
      _hashCode = letter1;
      return _hashCode!;
    }
    return letter1 * 17 + path.hashCode;
  }
}