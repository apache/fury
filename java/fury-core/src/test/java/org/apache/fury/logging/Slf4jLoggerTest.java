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
