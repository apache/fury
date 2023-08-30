package io.fury.resolver;

import static org.testng.Assert.*;

import io.fury.Fury;
import io.fury.exception.InsecureException;
import org.testng.annotations.Test;

public class AllowListCheckerTest {

  @Test
  public void testCheckClass() {
    {
      Fury fury = Fury.builder().requireClassRegistration(false).build();
      AllowListChecker checker = new AllowListChecker(AllowListChecker.CheckLevel.STRICT);
      fury.getClassResolver().setClassChecker(checker);
      assertThrows(InsecureException.class, () -> fury.serialize(new AllowListCheckerTest()));
      checker.allowClass(AllowListCheckerTest.class.getName());
      fury.serialize(new AllowListCheckerTest());
      checker.addListener(fury.getClassResolver());
      checker.disallowClass(AllowListCheckerTest.class.getName());
      assertThrows(InsecureException.class, () -> fury.serialize(new AllowListCheckerTest()));
    }
    {
      Fury fury = Fury.builder().requireClassRegistration(false).build();
      AllowListChecker checker = new AllowListChecker(AllowListChecker.CheckLevel.WARN);
      fury.getClassResolver().setClassChecker(checker);
      checker.addListener(fury.getClassResolver());
      fury.serialize(new AllowListCheckerTest());
      checker.disallowClass(AllowListCheckerTest.class.getName());
      assertThrows(InsecureException.class, () -> fury.serialize(new AllowListCheckerTest()));
    }
  }

  @Test
  public void testCheckClassWildcard() {
    {
      Fury fury = Fury.builder().requireClassRegistration(false).build();
      AllowListChecker checker = new AllowListChecker(AllowListChecker.CheckLevel.STRICT);
      fury.getClassResolver().setClassChecker(checker);
      checker.addListener(fury.getClassResolver());
      assertThrows(InsecureException.class, () -> fury.serialize(new AllowListCheckerTest()));
      checker.allowClass("io.fury.*");
      fury.serialize(new AllowListCheckerTest());
      checker.disallowClass("io.fury.*");
      assertThrows(InsecureException.class, () -> fury.serialize(new AllowListCheckerTest()));
    }
    {
      Fury fury = Fury.builder().requireClassRegistration(false).build();
      AllowListChecker checker = new AllowListChecker(AllowListChecker.CheckLevel.WARN);
      fury.getClassResolver().setClassChecker(checker);
      checker.addListener(fury.getClassResolver());
      fury.serialize(new AllowListCheckerTest());
      checker.disallowClass("io.fury.*");
      assertThrows(InsecureException.class, () -> fury.serialize(new AllowListCheckerTest()));
    }
  }
}
