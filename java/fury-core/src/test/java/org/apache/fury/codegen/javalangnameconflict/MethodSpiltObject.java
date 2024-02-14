package org.apache.fury.codegen.javalangnameconflict;

import java.util.List;
import org.apache.fury.test.bean.BeanA;

/** A class used to test `Object` type conflict in #1370. */
public class MethodSpiltObject extends BeanA {
  private Object f1;
  public Object f2;
  private java.lang.Object f3;
  private List<java.lang.Object> f4;
  private List<Object> f5;
}
