final class TypeIntKey{
  final Type type;
  final int strIdentityHash;

  const TypeIntKey(this.type, this.strIdentityHash);

  @override
  int get hashCode => type.hashCode  ^ strIdentityHash.hashCode;

  @override
  bool operator ==(Object other) {
    return
      other is TypeIntKey &&
          other.type == type &&
          other.strIdentityHash == strIdentityHash;
  }
}