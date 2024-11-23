package org.apache.fury.exception;

public class ClassUnregisteredException extends FuryException {

  public ClassUnregisteredException(Class<?> cls) {
    this(cls.getName());
  }

  public ClassUnregisteredException(String qualifiedName) {
    super(String.format("Class %s is not registered", qualifiedName));
  }
}
