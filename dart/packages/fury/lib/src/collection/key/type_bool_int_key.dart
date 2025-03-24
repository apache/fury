final class TypeBoolIntKey{
  final Type type;
  final bool writeRef;
  final int strIdentityHash;

  const TypeBoolIntKey(this.type, this.writeRef, this.strIdentityHash);

  @override
  int get hashCode => type.hashCode  ^ strIdentityHash.hashCode ^ writeRef.hashCode;

  @override
  bool operator ==(Object other) {
    return
      other is TypeBoolIntKey &&
          other.type == type &&
          other.writeRef == writeRef &&
          other.strIdentityHash == strIdentityHash;
  }
}