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

package org.apache.fury.logging;

import org.testng.annotations.Test;

public class Slf4jLoggerTest {

  @Test
  public void testInfo() {
    Slf4jLogger logger = new Slf4jLogger((Slf4jLoggerTest.class));
    FuryLogger furyLogger = new FuryLogger((Slf4jLoggerTest.class));
    logger.info("testInfo");
    logger.info("testInfo {}", "placeHolder");
    logger.warn("testInfo {}", "placeHolder");
    logger.error("testInfo {}", "placeHolder", new Exception("test log"));
    furyLogger.info("testInfo");
    furyLogger.info("testInfo {}", "placeHolder");
    furyLogger.warn("testInfo {}", "placeHolder");
    furyLogger.error("testInfo {}", "placeHolder", new Exception("test log"));
  }
}
