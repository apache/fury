package io.fury.integration_tests;

public class Records {
  private record PrivateRecord(int f1) {}

  public record PublicRecord(int f1, PrivateRecord f2) {}

  public static Object createPrivateRecord(int f1) {
    return new PrivateRecord(f1);
  }

  public static Object createPublicRecord(int f1, Object privateObject) {
    return new PublicRecord(f1, (PrivateRecord) privateObject);
  }
}
