package io.fury.util;

/**
 * Check utils
 *
 * @author chaokunyang
 */
public class Preconditions {
  public static <T> T checkNotNull(T o) {
    if (o == null) {
      throw new NullPointerException();
    }
    return o;
  }

  public static <T> T checkNotNull(T o, String errorMessage) {
    if (o == null) {
      throw new NullPointerException(errorMessage);
    }
    return o;
  }

  public static void checkState(boolean expression) {
    if (!expression) {
      throw new IllegalStateException();
    }
  }

  public static void checkArgument(boolean b) {
    if (!b) {
      throw new IllegalArgumentException();
    }
  }

  public static void checkArgument(boolean b, Object errorMessage) {
    if (!b) {
      throw new IllegalArgumentException(String.valueOf(errorMessage));
    }
  }

  public static void checkArgument(boolean b, String errorMessage) {
    if (!b) {
      throw new IllegalArgumentException(errorMessage);
    }
  }

  /**
   * Ensures the truth of an expression involving one or more parameters to the calling method.
   * Workaround for guava before 20.0.
   */
  public static void checkArgument(
      boolean b, String errorMessageTemplate, Object errorMessageArg0, Object... errorMessageArgs) {
    if (!b) {
      Object[] args;
      if (errorMessageArgs != null) {
        args = new Object[errorMessageArgs.length + 1];
        args[0] = errorMessageArg0;
        System.arraycopy(errorMessageArgs, 0, args, 1, errorMessageArgs.length);
      } else {
        args = new Object[] {errorMessageArg0};
      }
      throw new IllegalArgumentException(String.format(errorMessageTemplate, args));
    }
  }
}
