/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fury.annotation;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import com.google.common.collect.Lists;
import java.util.List;
import lombok.Data;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FuryAnnotationTest extends FuryTestBase {

  @Data
  public static class BeanM {
    @FuryField(nullable = false)
    public Long f1;

    @FuryField(nullable = false)
    private Long f2;

    String s = "str";
    Short shortValue = Short.valueOf((short) 2);
    Byte byteValue = Byte.valueOf((byte) 3);
    Long longValue = Long.valueOf(4L);
    Boolean booleanValue = Boolean.TRUE;
    Float floatValue = Float.valueOf(5.0f);
    Double doubleValue = Double.valueOf(6.0);
    Character character = Character.valueOf('c');

    int i = 10;

    int i2;

    long l1;

    double d1;

    char c1;

    boolean b1;

    byte byte1;

    @FuryField(nullable = false)
    List<Integer> integerList = Lists.newArrayList(1);

    @FuryField(nullable = false)
    String s1 = "str";

    @FuryField(nullable = false)
    Short shortValue1 = Short.valueOf((short) 2);

    @FuryField(nullable = false)
    Byte byteValue1 = Byte.valueOf((byte) 3);

    @FuryField(nullable = false)
    Long longValue1 = Long.valueOf(4L);

    @FuryField(nullable = false)
    Boolean booleanValue1 = Boolean.TRUE;

    @FuryField(nullable = false)
    Float floatValue1 = Float.valueOf(5.0f);

    @FuryField(nullable = false)
    Double doubleValue1 = Double.valueOf(6.0);

    @FuryField(nullable = false)
    Character character1 = Character.valueOf('c');

    @FuryField(nullable = true)
    List<Integer> integerList1 = Lists.newArrayList(1);

    @FuryField(nullable = true)
    String s2 = "str";

    @FuryField(nullable = true)
    Short shortValue2 = Short.valueOf((short) 2);

    @FuryField(nullable = true)
    Byte byteValue2 = Byte.valueOf((byte) 3);

    @FuryField(nullable = true)
    Long longValue2 = Long.valueOf(4L);

    @FuryField(nullable = true)
    Boolean booleanValue2 = Boolean.TRUE;

    @FuryField(nullable = true)
    Float floatValue2 = Float.valueOf(5.0f);

    @FuryField(nullable = true)
    Double doubleValue2 = Double.valueOf(6.0);

    @FuryField(nullable = true)
    Character character2 = Character.valueOf('c');

    public BeanM() {
      this.f1 = 1L;
      this.f2 = 1L;
    }
  }

  @Data
  public static class BeanN {
    public long f1;
    private long f2;
  }

  @Data
  public static class BeanM1 {

    @FuryField(nullable = false)
    private BeanN beanN;
  }

  @Test(dataProvider = "basicMultiConfigFury")
  public void testFuryFieldAnnotation(
      boolean trackingRef,
      boolean codeGen,
      boolean scopedMetaShare,
      CompatibleMode compatibleMode) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(trackingRef)
            .requireClassRegistration(false)
            .withCodegen(codeGen)
            .withCompatibleMode(compatibleMode)
            .withScopedMetaShare(scopedMetaShare)
            .build();
    BeanM o = new BeanM();
    byte[] bytes = fury.serialize(o);
    final Object deserialize = fury.deserialize(bytes);
    Assert.assertEquals(o, deserialize);
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testFuryFieldAnnotationException(boolean referenceTracking) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false)
            .withCodegen(false)
            .build();
    BeanM1 o1 = new BeanM1();
    if (referenceTracking) {
      assertEquals(serDe(fury, o1), o1);
    } else {
      assertThrows(NullPointerException.class, () -> fury.serialize(o1));
    }
  }
}
