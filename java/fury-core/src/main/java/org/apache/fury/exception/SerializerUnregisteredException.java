package org.apache.fury.exception;

public class SerializerUnregisteredException extends FuryException {

  public SerializerUnregisteredException(String qualifiedName) {
    super(String.format("Class %s is not registered with a serializer", qualifiedName));
  }
}
