package org.apache.fury.resolver;

class ClassNameBytes {
  private final long packageHash;
  private final long classNameHash;

  ClassNameBytes(long packageHash, long classNameHash) {
    this.packageHash = packageHash;
    this.classNameHash = classNameHash;
  }

  @Override
  public boolean equals(Object o) {
    // ClassNameBytes is used internally, skip
    ClassNameBytes that = (ClassNameBytes) o;
    return packageHash == that.packageHash && classNameHash == that.classNameHash;
  }

  @Override
  public int hashCode() {
    int result = 31 + (int) (packageHash ^ (packageHash >>> 32));
    result = result * 31 + (int) (classNameHash ^ (classNameHash >>> 32));
    return result;
  }
}
