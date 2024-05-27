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

package org.test;

import java.io.Serializable;
import java.util.List;
import org.apache.fury.Fury;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;
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

  // test for class name same with package name:
  // https://github.com/janino-compiler/janino/issues/165
  @Test
  public void testOrgPackage() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            // Allow to deserialize objects unknown types,more flexible but less secure.
            .requireClassRegistration(false)
            .withDeserializeNonexistentClass(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withRefTracking(true)
            .build();

    // If the class name is not Org, it can be serialized normally
    byte[] bytes = fury.serialize(new Org());
    Object o = fury.deserialize(bytes);
    Assert.assertEquals(o.getClass(), Org.class);
  }
}
