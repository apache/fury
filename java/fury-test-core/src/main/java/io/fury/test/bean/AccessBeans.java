package io.fury.test.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

public class AccessBeans {
  @Data
  @AllArgsConstructor
  private static class PrivateClass {
    public int f1;
    int f2;
    private int f3;
  }

  @Data
  @AllArgsConstructor
  private static final class FinalPrivateClass {
    public int f1;
    int f2;
    private int f3;
  }

  @Data
  @AllArgsConstructor
  static class DefaultLevelClass {
    public int f1;
    int f2;
    private int f3;
  }

  @Data
  @AllArgsConstructor
  public static class PublicClass {
    public int f1;
    int f2;
    private int f3;
    private DefaultLevelClass f4;
    private PrivateClass f5;
    private FinalPrivateClass f6;
  }

  public static PrivateClass createPrivateClassObject() {
    return new PrivateClass(1, 2, 3);
  }

  public static FinalPrivateClass createPrivateFinalClassObject() {
    return new FinalPrivateClass(1, 2, 3);
  }

  public static DefaultLevelClass createDefaultLevelClassObject() {
    return new DefaultLevelClass(4, 5, 6);
  }

  public static PublicClass createPublicClassObject() {
    return new PublicClass(
        1,
        2,
        3,
        createDefaultLevelClassObject(),
        createPrivateClassObject(),
        createPrivateFinalClassObject());
  }
}
