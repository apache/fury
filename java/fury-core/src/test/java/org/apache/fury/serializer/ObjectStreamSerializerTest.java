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

package org.apache.fury.serializer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamField;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import lombok.EqualsAndHashCode;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.util.Preconditions;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ObjectStreamSerializerTest extends FuryTestBase {

  @EqualsAndHashCode
  public static class WriteObjectTestClass implements Serializable {
    int count;
    char[] value;

    public WriteObjectTestClass(char[] value) {
      this.count = value.length;
      this.value = value;
    }

    private void writeObject(ObjectOutputStream s) throws IOException {
      s.defaultWriteObject();
      s.writeInt(count);
      s.writeObject(value);
    }

    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
      s.defaultReadObject();
      count = s.readInt();
      value = (char[]) s.readObject();
    }
  }

  @Test(dataProvider = "javaFury")
  public void testJDKCompatibleCommon(Fury fury) {
    WriteObjectTestClass o = new WriteObjectTestClass(new char[] {'a', 'b'});
    fury.registerSerializer(
        WriteObjectTestClass.class, new ObjectStreamSerializer(fury, WriteObjectTestClass.class));
    serDeCheckSerializer(fury, o, "ObjectStreamSerializer");
    fury.registerSerializer(
        StringBuilder.class, new ObjectStreamSerializer(fury, StringBuilder.class));
    assertSame(
        fury.getClassResolver().getSerializerClass(StringBuilder.class),
        ObjectStreamSerializer.class);
    StringBuilder buf = (StringBuilder) serDe(fury, new StringBuilder("abc"));
    assertEquals(buf.toString(), "abc");
  }

  @Test(dataProvider = "javaFury")
  public void testDispatch(Fury fury) {
    WriteObjectTestClass o = new WriteObjectTestClass(new char[] {'a', 'b'});
    serDeCheckSerializer(fury, o, "ObjectStreamSerializer");
  }

  @EqualsAndHashCode(callSuper = true)
  public static class WriteObjectTestClass2 extends WriteObjectTestClass {
    private final String data;

    public WriteObjectTestClass2(char[] value, String data) {
      super(value);
      this.data = data;
    }

    private void writeObject(ObjectOutputStream s) throws IOException {
      s.defaultWriteObject();
      s.writeInt(100);
    }

    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
      // defaultReadObject compatible with putFields
      s.defaultReadObject();
      Preconditions.checkArgument(s.readInt() == 100);
    }
  }

  @EqualsAndHashCode(callSuper = true)
  public static class WriteObjectTestClass3 extends WriteObjectTestClass {
    private String data;

    private static final ObjectStreamField[] serialPersistentFields = {
      new ObjectStreamField("notExist1", Integer.TYPE),
      new ObjectStreamField("notExist2", String.class)
    };

    public WriteObjectTestClass3(char[] value, String data) {
      super(value);
      this.data = data;
    }

    private void writeObject(ObjectOutputStream s) throws IOException {
      s.putFields().put("notExist1", 100);
      s.putFields().put("notExist2", "abc");
      s.writeFields();
      s.writeUTF(data);
    }

    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
      // defaultReadObject compatible with putFields
      s.defaultReadObject();
      data = s.readUTF();
    }
  }

  @Test(dataProvider = "javaFury")
  public void testJDKCompatiblePutFields(Fury fury) {
    fury.registerSerializer(
        StringBuffer.class, new ObjectStreamSerializer(fury, StringBuffer.class));
    assertSame(
        fury.getClassResolver().getSerializerClass(StringBuffer.class),
        ObjectStreamSerializer.class);
    // test `putFields`
    StringBuffer newStringBuffer = (StringBuffer) serDe(fury, new StringBuffer("abc"));
    assertEquals(newStringBuffer.toString(), "abc");
    BigInteger bigInteger = BigInteger.valueOf(1000);
    fury.registerSerializer(BigInteger.class, new ObjectStreamSerializer(fury, BigInteger.class));
    serDeCheck(fury, bigInteger);
    fury.registerSerializer(InetAddress.class, new ObjectStreamSerializer(fury, InetAddress.class));
    fury.registerSerializer(
        Inet4Address.class, new ObjectStreamSerializer(fury, Inet4Address.class));
    InetAddress inetAddress = InetAddress.getLoopbackAddress();
    serDeCheck(fury, inetAddress);
    WriteObjectTestClass2 testClassObj2 = new WriteObjectTestClass2(new char[] {'a', 'b'}, "abc");
    fury.registerSerializer(
        WriteObjectTestClass2.class, new ObjectStreamSerializer(fury, WriteObjectTestClass2.class));
    serDeCheck(fury, testClassObj2);
    // test defaultReadObject compatible with putFields.
    WriteObjectTestClass3 testClassObj3 = new WriteObjectTestClass3(new char[] {'a', 'b'}, "abc");
    fury.registerSerializer(
        WriteObjectTestClass3.class, new ObjectStreamSerializer(fury, WriteObjectTestClass3.class));
    serDeCheck(fury, testClassObj3);
  }

  @Test(dataProvider = "javaFury")
  public void testJDKCompatibleMap(Fury fury) {
    ImmutableMap<String, Integer> mapData = ImmutableMap.of("k1", 1, "k2", 2);
    {
      ObjectStreamSerializer serializer = new ObjectStreamSerializer(fury, ConcurrentHashMap.class);
      MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
      ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>(mapData);
      fury.getRefResolver().writeRefOrNull(buffer, map);
      serializer.write(buffer, map);
      fury.getRefResolver().tryPreserveRefId(buffer);
      Object newMap = serializer.read(buffer);
      assertEquals(buffer.writerIndex(), buffer.readerIndex());
      assertEquals(newMap, map);
      // ConcurrentHashMap internal structure may use jdk serialization, which will update
      // SerializationContext.
      fury.reset();
    }
    {
      fury.registerSerializer(
          ConcurrentHashMap.class, new ObjectStreamSerializer(fury, ConcurrentHashMap.class));
      serDeCheck(fury, new ConcurrentHashMap<>(mapData));
      assertSame(
          fury.getClassResolver().getSerializer(ConcurrentHashMap.class).getClass(),
          ObjectStreamSerializer.class);
    }
    {
      // ImmutableMap use writeReplace, which needs special handling.
      Map<String, Integer> map = new HashMap<>(mapData);
      fury.registerSerializer(map.getClass(), new ObjectStreamSerializer(fury, map.getClass()));
      serDeCheck(fury, map);
    }
  }

  @Test(dataProvider = "javaFury")
  public void testJDKCompatibleList(Fury fury) {
    fury.registerSerializer(ArrayList.class, new ObjectStreamSerializer(fury, ArrayList.class));
    List<String> list = new ArrayList<>(ImmutableList.of("a", "b", "c", "d"));
    serDeCheck(fury, list);
    fury.registerSerializer(LinkedList.class, new ObjectStreamSerializer(fury, LinkedList.class));
    serDeCheck(fury, new LinkedList<>(list));
    fury.registerSerializer(Vector.class, new ObjectStreamSerializer(fury, Vector.class));
    serDeCheck(fury, new Vector<>(list));
  }

  @Test(dataProvider = "enableCodegen")
  public void testJDKCompatibleCircularReference(boolean enableCodegen) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .requireClassRegistration(false)
            .withRefTracking(true)
            .withCodegen(enableCodegen)
            .build();
    {
      ObjectStreamSerializer serializer = new ObjectStreamSerializer(fury, ConcurrentHashMap.class);
      MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
      ConcurrentHashMap<String, Object> map =
          new ConcurrentHashMap<>(
              ImmutableMap.of(
                  "k1", 1,
                  "k2", 2));
      map.put("k3", map);
      fury.getRefResolver().writeRefOrNull(buffer, map);
      serializer.write(buffer, map);
      fury.getRefResolver().tryPreserveRefId(buffer);
      @SuppressWarnings("unchecked")
      ConcurrentHashMap<String, Object> newMap =
          (ConcurrentHashMap<String, Object>) serializer.read(buffer);
      assertEquals(buffer.writerIndex(), buffer.readerIndex());
      assertSame(newMap.get("k3"), newMap);
      assertEquals(newMap.get("k2"), map.get("k2"));
    }
  }

  public abstract static class ValidationTestClass1 implements Serializable {
    transient int state;
    String str;

    public ValidationTestClass1(String str) {
      this.str = str;
    }

    private void writeObject(ObjectOutputStream s) throws IOException {
      s.defaultWriteObject();
    }

    private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
      s.defaultReadObject();
      s.registerValidation(() -> ValidationTestClass1.this.state = getStateInternal(), 0);
    }

    protected abstract int getStateInternal();
  }

  public static class ValidationTestClass2 extends ValidationTestClass1 {
    int realState;

    public ValidationTestClass2(String str, int realState) {
      super(str);
      this.realState = realState;
    }

    @Override
    protected int getStateInternal() {
      return realState;
    }
  }

  @Test(dataProvider = "javaFury")
  public void testObjectInputValidation(Fury fury) {
    // ObjectStreamSerializer serializer = new ObjectStreamSerializer(fury, HTMLDocument.class);
    // MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    // HTMLDocument document = new HTMLDocument();
    // fury.getRefResolver().writeRefOrNull(buffer, document);
    // serializer.write(buffer, document);
    // fury.getRefResolver().tryPreserveRefId(buffer);
    // HTMLDocument newDocument = (HTMLDocument) serializer.read(buffer);
    fury.registerSerializer(
        ValidationTestClass2.class, new ObjectStreamSerializer(fury, ValidationTestClass2.class));
    int realState = 100;
    String str = "abc";
    ValidationTestClass2 obj = new ValidationTestClass2(str, realState);
    ValidationTestClass2 obj2 = (ValidationTestClass2) serDe(fury, obj);
    assertEquals(obj2.realState, realState);
    assertEquals(obj2.str, str);
    // assert validation callback work.
    assertEquals(obj2.state, realState);
  }

  @EqualsAndHashCode(callSuper = true)
  public static class WriteObjectTestClass4 extends WriteObjectTestClass {

    public WriteObjectTestClass4(char[] value) {
      super(value);
    }

    private Object writeReplace() {
      return this;
    }

    private Object readResolve() {
      return this;
    }
  }

  @Test(dataProvider = "javaFury")
  public void testWriteObjectReplace(Fury fury) throws MalformedURLException {
    Assert.assertEquals(
        serDeCheckSerializer(fury, new URL("http://test"), "ReplaceResolve"),
        new URL("http://test"));
    WriteObjectTestClass4 testClassObj4 = new WriteObjectTestClass4(new char[] {'a', 'b'});
    fury.registerSerializer(
        WriteObjectTestClass4.class, new ObjectStreamSerializer(fury, WriteObjectTestClass4.class));
    serDeCheckSerializer(fury, testClassObj4, "ObjectStreamSerializer");
  }

  // TODO(chaokunyang) add `readObjectNoData` test for class inheritance change.
  // @Test
  public void testReadObjectNoData() {}
}
