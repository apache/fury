package io.fury.util;

public class ExceptionUtils {
  private static final long detailMessageOffset =
      ReflectionUtils.getFieldOffset(Throwable.class, "detailMessage");

  /**
   * Try to set `StackOverflowError` exception message. Returns passed exception if set succeed, or
   * null if failed.
   */
  public static StackOverflowError trySetStackOverflowErrorMessage(
      StackOverflowError e, String message) {
    if (detailMessageOffset != 0) {
      Platform.putObject(e, detailMessageOffset, message);
      return e;
    } else {
      return null;
    }
  }
}
