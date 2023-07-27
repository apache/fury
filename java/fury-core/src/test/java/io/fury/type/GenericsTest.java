/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.type;

import com.google.common.collect.ImmutableList;
import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.TestUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("rawtypes")
public class GenericsTest extends FuryTestBase {

  public static class Test1<FLOAT, STRING, NULL> {
    public ArrayList<String> fromField1;
    public ArrayList<FLOAT> fromSubClass;
    public ArrayList<STRING> fromSubSubClass;
    public HashMap<FLOAT, STRING> multipleFromSubClasses;
    public HashMap<FLOAT, NULL> multipleWithUnknown;
    public FLOAT[] arrayFromSubClass;
    public STRING[] arrayFromSubSubClass;
  }

  public static class Test2<STRING, NULL, ARRAYLIST> extends Test1<Float, STRING, NULL> {
    public STRING known;
    public STRING[] array1;
    public STRING[][] array2;
    public ArrayList<NULL> unknown1;
    public NULL[] arrayUnknown1;
    public ArrayList<String> fromField2;
    public ArrayList<STRING>[] arrayWithTypeVar;
    public ArrayList<ArrayList<String>> fromFieldNested;
    public ARRAYLIST parameterizedTypeFromSubClass;
    public ArrayList<ARRAYLIST>[] parameterizedArrayFromSubClass;
  }

  public static class Test3<DOUBLE extends Number & Comparable, NULL, LONG>
      extends Test2<String, NULL, ArrayList<LONG>> {
    public ArrayList<String> fromField3;
    public ArrayList raw;
    public NULL unknown2;
    public NULL[] arrayUnknown2;
    public ArrayList<?> unboundWildcard;
    public ArrayList<? extends Number> upperBound;
    public ArrayList<? super Integer> lowerBound;
    public ArrayList<DOUBLE> multipleUpperBounds;
  }

  public static class Test4<NULL> extends Test3<Double, NULL, Long> {}

  public static class Test5 {
    public List<String> f1;
  }

  @Test
  public void testGenericType() throws Exception {
    // Test1
    Assert.assertEquals(
        GenericType.build(Test4.class, Test1.class.getField("fromField1").getGenericType())
            .getTypeParameter0()
            .getCls(),
        String.class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test1.class.getField("fromSubClass").getGenericType())
            .getTypeParameter0()
            .getCls(),
        Float.class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test1.class.getField("fromSubSubClass").getGenericType())
            .getTypeParameter0()
            .getCls(),
        String.class);
    Assert.assertEquals(
        GenericType.build(
                Test4.class, Test1.class.getField("multipleFromSubClasses").getGenericType())
            .getTypeParameter0()
            .getCls(),
        Float.class);
    Assert.assertEquals(
        GenericType.build(
                Test4.class, Test1.class.getField("multipleFromSubClasses").getGenericType())
            .getTypeParameter1()
            .getCls(),
        String.class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test1.class.getField("multipleWithUnknown").getGenericType())
            .getTypeParameter0()
            .getCls(),
        Float.class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test1.class.getField("multipleWithUnknown").getGenericType())
            .getTypeParameter1()
            .getCls(),
        Object.class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test1.class.getField("arrayFromSubClass").getGenericType())
            .getCls(),
        Float[].class);
    Assert.assertEquals(
        GenericType.build(
                Test4.class, Test1.class.getField("arrayFromSubSubClass").getGenericType())
            .getCls(),
        String[].class);

    // Test2
    Assert.assertEquals(
        GenericType.build(Test4.class, Test2.class.getField("known").getGenericType()).getCls(),
        String.class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test2.class.getField("array1").getGenericType()).getCls(),
        String[].class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test2.class.getField("array2").getGenericType()).getCls(),
        String[][].class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test2.class.getField("unknown1").getGenericType())
            .getTypeParameter0()
            .getCls(),
        Object.class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test2.class.getField("arrayUnknown1").getGenericType())
            .getCls(),
        Object[].class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test2.class.getField("fromField2").getGenericType())
            .getTypeParameter0()
            .getCls(),
        String.class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test2.class.getField("arrayWithTypeVar").getGenericType())
            .getTypeParameter0()
            .getTypeParameter0()
            .getCls(),
        String.class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test2.class.getField("fromFieldNested").getGenericType())
            .getTypeParameter0()
            .getTypeParameter0()
            .getCls(),
        String.class);
    Assert.assertEquals(
        GenericType.build(
                Test4.class, Test2.class.getField("parameterizedTypeFromSubClass").getGenericType())
            .getCls(),
        ArrayList.class);
    Assert.assertEquals(
        GenericType.build(
                Test4.class,
                Test2.class.getField("parameterizedArrayFromSubClass").getGenericType())
            .getTypeParameter0()
            .getCls(),
        ArrayList.class);

    // Test3
    Assert.assertEquals(
        GenericType.build(Test4.class, Test3.class.getField("fromField3").getGenericType())
            .getTypeParameter0()
            .getCls(),
        String.class);
    Assert.assertNull(
        GenericType.build(Test4.class, Test3.class.getField("raw").getGenericType())
            .getTypeParameter0());
    Assert.assertEquals(
        GenericType.build(Test4.class, Test3.class.getField("unknown2").getGenericType()).getCls(),
        Object.class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test3.class.getField("arrayUnknown2").getGenericType())
            .getCls(),
        Object[].class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test3.class.getField("unboundWildcard").getGenericType())
            .getTypeParameter0()
            .getCls(),
        Object.class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test3.class.getField("upperBound").getGenericType())
            .getTypeParameter0()
            .getCls(),
        Number.class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test3.class.getField("lowerBound").getGenericType())
            .getTypeParameter0()
            .getCls(),
        Object.class);
    Assert.assertEquals(
        GenericType.build(Test4.class, Test3.class.getField("multipleUpperBounds").getGenericType())
            .getTypeParameter0()
            .getCls(),
        Double.class);

    // Test5
    Assert.assertEquals(
        GenericType.build(Test5.class, Test5.class.getField("f1").getGenericType())
            .getTypeParameter0()
            .getCls(),
        String.class);
  }

  @Test
  public void testGenerics() throws NoSuchFieldException {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).build();
    Generics generics = new Generics(fury);
    {
      GenericType genericType =
          GenericType.build(Test4.class, Test2.class.getField("fromFieldNested").getGenericType());
      // push generics in outer serialization.
      generics.pushGenericType(genericType);
      // increase serialization depth.
      increaseFuryDepth(fury, 1);
      // get generics in inner serialization.
      GenericType genericType1 = generics.nextGenericType();
      Assert.assertSame(genericType1, genericType);
      increaseFuryDepth(fury, -1);
      generics.popGenericType();
    }
    {
      for (String fieldName : new String[] {"fromField2", "arrayWithTypeVar", "fromFieldNested"}) {
        GenericType genericType =
            GenericType.build(Test4.class, Test2.class.getField(fieldName).getGenericType());
        generics.pushGenericType(genericType);
        increaseFuryDepth(fury, 1);
      }
      for (String fieldName :
          ImmutableList.of("fromField2", "arrayWithTypeVar", "fromFieldNested").reverse()) {
        GenericType genericType =
            GenericType.build(Test4.class, Test2.class.getField(fieldName).getGenericType());
        GenericType genericType1 = generics.nextGenericType();
        Assert.assertEquals(genericType1.typeToken, genericType.typeToken);
        increaseFuryDepth(fury, -1);
        generics.popGenericType();
      }
    }
    Assert.assertEquals(TestUtils.getFieldValue(generics, "genericTypesSize"), Integer.valueOf(0));
    GenericType[] genericTypes = TestUtils.getFieldValue(generics, "genericTypes");
    Assert.assertNull(genericTypes[0]);
    Assert.assertNull(genericTypes[1]);
  }
}
