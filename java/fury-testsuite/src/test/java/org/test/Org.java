package org.test;

import io.fury.Fury;
import io.fury.ThreadSafeFury;
import io.fury.config.CompatibleMode;
import io.fury.config.Language;
import java.io.Serializable;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class Org implements Serializable {
  private static final long serialVersionUID = 1L;
  // constructor
  public Org() {}

  List<Org> children;

  public List<Org> getChildren() {
    return children;
  }

  public void setChildren(List<Org> children) {
    this.children = children;
  }

  // test for https://github.com/alipay/fury/issues/1005
  @Test
  public void testOrgPackage() {
    ThreadSafeFury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            // Allow to deserialize objects unknown types,more flexible but less secure.
            .requireClassRegistration(false)
            .withDeserializeUnexistedClass(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withRefTracking(true)
            .buildThreadSafeFury();

    // If the class name is not Org, it can be serialized normally
    byte[] bytes = fury.serialize(new Org());
    Object o = fury.deserialize(bytes);
    Assert.assertEquals(o.getClass(), Org.class);
  }
}
