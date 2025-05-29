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

package org.apache.fory.annotation;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.Lists;
import java.util.List;
import lombok.Data;
import org.apache.fory.Fory;
import org.apache.fory.ForyTestBase;
import org.apache.fory.config.CompatibleMode;
import org.apache.fory.config.Language;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ForyAnnotationTest extends ForyTestBase {

  @Data
  public static class BeanM {
    @ForyField(nullable = false)
    public Long f1;

    @ForyField(nullable = false)
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

    @ForyField int i3 = 10;

    @ForyField List<Integer> integerList = Lists.newArrayList(1);

    @ForyField String s1 = "str";

    @ForyField(nullable = false)
    Short shortValue1 = Short.valueOf((short) 2);

    @ForyField(nullable = false)
    Byte byteValue1 = Byte.valueOf((byte) 3);

    @ForyField(nullable = false)
    Long longValue1 = Long.valueOf(4L);

    @ForyField(nullable = false)
    Boolean booleanValue1 = Boolean.TRUE;

    @ForyField(nullable = false)
    Float floatValue1 = Float.valueOf(5.0f);

    @ForyField(nullable = false)
    Double doubleValue1 = Double.valueOf(6.0);

    @ForyField(nullable = false)
    Character character1 = Character.valueOf('c');

    @ForyField(nullable = true)
    List<Integer> integerList1 = Lists.newArrayList(1);

    @ForyField(nullable = true)
    String s2 = "str";

    @ForyField(nullable = true)
    Short shortValue2 = Short.valueOf((short) 2);

    @ForyField(nullable = true)
    Byte byteValue2 = Byte.valueOf((byte) 3);

    @ForyField(nullable = true)
    Long longValue2 = Long.valueOf(4L);

    @ForyField(nullable = true)
    Boolean booleanValue2 = Boolean.TRUE;

    @ForyField(nullable = true)
    Float floatValue2 = Float.valueOf(5.0f);

    @ForyField(nullable = true)
    Double doubleValue2 = Double.valueOf(6.0);

    @ForyField(nullable = true)
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

    @ForyField private BeanN beanN = new BeanN();
  }

  @Test(dataProvider = "basicMultiConfigFory")
  public void testForyFieldAnnotation(
      boolean trackingRef,
      boolean codeGen,
      boolean scopedMetaShare,
      CompatibleMode compatibleMode) {
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(trackingRef)
            .requireClassRegistration(false)
            .withCodegen(codeGen)
            .withCompatibleMode(compatibleMode)
            .withScopedMetaShare(scopedMetaShare)
            .build();
    BeanM o = new BeanM();
    byte[] bytes = fory.serialize(o);
    final Object deserialize = fory.deserialize(bytes);
    Assert.assertEquals(o, deserialize);
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testForyFieldAnnotationException(boolean referenceTracking) {
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false)
            .withCodegen(false)
            .build();
    BeanM1 o1 = new BeanM1();
    assertEquals(serDe(fory, o1), o1);
  }
}
