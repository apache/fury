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

package io.fury.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.google.common.base.CaseFormat;
import org.testng.annotations.Test;

public class StringUtilsTest {

  @Test
  public void testEncodeHexString() {
    assertEquals(
        StringUtils.encodeHexString(new byte[] {1, 2, 10, 20, 30, 40, 50}), "01020a141e2832");
  }

  @Test
  public void testRandom() {
    StringUtils.random(40);
    assertEquals(StringUtils.random(4, 7), "#,q7");
  }

  @Test
  public void testSnake() {
    assertEquals(
        CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, "ABCDEfgHij"), "a_b_c_d_efg_hij");
    assertEquals(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, "to_snake"), "to_snake");
    assertEquals(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, "ABC"), "a_b_c");
  }

  @Test
  public void testFormat() {
    assertEquals(StringUtils.format("${a}, ${b}", "a", 1, "b", "abc"), "1, abc");
  }

  @Test
  public void testStripBlankLines() {
    assertEquals(StringUtils.stripBlankLines("a\n \nb\n"), "a\nb\n");
  }

  @Test
  public void testCapitalize() {
    assertEquals(StringUtils.capitalize("abc"), "Abc");
    assertEquals(StringUtils.uncapitalize("Abc"), "abc");
  }

  @Test
  public void testIsBlank() {
    assertFalse(StringUtils.isBlank("abc"));
    assertTrue(StringUtils.isNotBlank("abc"));
    assertTrue(StringUtils.isBlank("   "));
    assertTrue(StringUtils.isBlank(null));
  }
}
