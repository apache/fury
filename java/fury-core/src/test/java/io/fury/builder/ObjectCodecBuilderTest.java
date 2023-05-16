package io.fury.builder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.codegen.CodeGenerator;
import io.fury.codegen.CompileUnit;
import io.fury.codegen.JaninoUtils;
import io.fury.test.bean.BeanA;
import io.fury.test.bean.BeanB;
import io.fury.test.bean.Foo;
import io.fury.test.bean.Struct;
import lombok.Data;
import org.codehaus.janino.ByteArrayClassLoader;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ObjectCodecBuilderTest extends FuryTestBase {

  @Test(dataProvider = "compressNumber")
  public void genCode(boolean compressNumber) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(false)
            .withNumberCompressed(compressNumber)
            .disableSecureMode()
            .build();
    new ObjectCodecBuilder(Foo.class, fury).genCode();
    // System.out.println(code);
    new ObjectCodecBuilder(BeanA.class, fury).genCode();
    new ObjectCodecBuilder(BeanB.class, fury).genCode();
    new ObjectCodecBuilder(Struct.createStructClass("", 1), fury).genCode();
  }

  @Test
  public void testDefaultPackage() throws Exception {
    CompileUnit unit =
        new CompileUnit(
            "",
            "A",
            ("" + "public class A {\n" + "  public int f1;\n" + "  public Object f2;\n" + "}"));
    ByteArrayClassLoader classLoader =
        JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), unit);
    Class<?> clz = classLoader.loadClass("A");
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(true)
            .withClassLoader(clz.getClassLoader())
            .disableSecureMode()
            .build();
    Object obj = clz.newInstance();
    Field f1 = clz.getDeclaredField("f1");
    f1.setAccessible(true);
    f1.set(obj, 2);
    Object newObj = fury.deserialize(fury.serialize(obj));
    Assert.assertEquals(f1.get(newObj), 2);
  }

  @DataProvider(name = "codecConfig")
  public static Object[][] codecConfig() {
    return Sets.cartesianProduct(
            ImmutableSet.of(true, false), // referenceTracking
            ImmutableSet.of(true, false), // basicTypesReferenceIgnored
            ImmutableSet.of(true, false), // compressNumber
            ImmutableSet.of(1, 4, 7) // structFieldsRepeat
            )
        .stream()
        .map(List::toArray)
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "codecConfig")
  public void testSeqCodec(
      boolean referenceTracking,
      boolean basicTypesRefIgnored,
      boolean compressNumber,
      int fieldsRepeat) {
    Class<?> structClass = Struct.createStructClass("Struct" + fieldsRepeat, fieldsRepeat);
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(referenceTracking)
            .withClassLoader(structClass.getClassLoader())
            .ignoreBasicTypesReference(basicTypesRefIgnored)
            .withNumberCompressed(compressNumber)
            .disableSecureMode()
            .build();
    Object struct = Struct.createPOJO(structClass);
    Assert.assertEquals(fury.deserialize(fury.serialize(struct)), struct);
    checkMethodSize(structClass, fury);
  }

  private void checkMethodSize(Class<?> clz, Fury fury) {
    ObjectCodecBuilder codecBuilder = new ObjectCodecBuilder(clz, fury);
    CompileUnit compileUnit =
        new CompileUnit(
            CodeGenerator.getPackage(clz), codecBuilder.codecClassName(clz), codecBuilder::genCode);
    byte[] bytecode =
        JaninoUtils.toBytecode(clz.getClassLoader(), compileUnit).values().iterator().next();
    JaninoUtils.CodeStats classStats = JaninoUtils.getClassStats(bytecode);
    System.out.println(classStats);
    classStats.methodsSize.entrySet().stream()
        .filter(e -> !e.getKey().equals("<init>"))
        .forEach(
            e ->
                Assert.assertTrue(
                    e.getValue() < 325,
                    String.format("Method %s has size %d > 325", e.getKey(), e.getValue())));
  }

  @Data
  public static class NestedContainer {
    private List<List<String>> list1;
    public Map<String, Set<List<String>>> map1;
  }

  @Test
  public void testNestedContainer() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    NestedContainer nestedContainer = new NestedContainer();
    List<List<String>> list1 = new ArrayList<>();
    list1.add(new ArrayList<>(ImmutableList.of("a", "b")));
    list1.add(new ArrayList<>(ImmutableList.of("a", "b")));
    nestedContainer.list1 = list1;
    Map<String, Set<List<String>>> map1 = new HashMap<>();
    map1.put("k1", new HashSet<>(list1));
    map1.put("k2", new HashSet<>(list1));
    nestedContainer.map1 = map1;
    serDeCheck(fury, nestedContainer);
    checkMethodSize(NestedContainer.class, fury);
  }
}
