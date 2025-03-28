// Currently not in use
class Type3StringKey{
  final String name;
  final String scheme; // It seems there are only two types: dart and package
  final String path;
  
  int? _hashCode;

  Type3StringKey(this.name, this.scheme, this.path);

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is Type3StringKey &&
          runtimeType == other.runtimeType &&
          name == other.name &&
          scheme == other.scheme &&
          path == other.path;

  @override
  int get hashCode {
    if (_hashCode != null) return _hashCode!;

    // In practice, names are mostly different, so the name needs to be given enough importance
    int nameHash = name.hashCode;
    int len1 = path.length;

    int letter1 = scheme.isNotEmpty ? scheme.codeUnitAt(0) : 32;
    int letter2 = path.isNotEmpty ? path.codeUnitAt(0) : 32;
    int letter3 = path.isNotEmpty ? path.codeUnitAt(path.length ~/ 2) : 32;
    int letter4 = path.isNotEmpty ? path.codeUnitAt((path.length ~/ 7) * 5) : 32;

    int hash = 17;
    hash = hash * 31 + nameHash;
    hash = hash * 31 + len1;
    hash = hash * 31 + letter1;
    hash = hash * 31 + letter2;
    hash = hash * 31 + letter3;
    hash = hash * 31 + letter4;

    _hashCode = hash;
    return hash;
  }
}
