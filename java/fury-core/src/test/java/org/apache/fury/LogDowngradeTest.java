package org.apache.fury;


import org.apache.fury.config.ExceptionLogMode;
import org.apache.fury.config.Language;
import org.testng.annotations.Test;

import java.util.Base64;


public class LogDowngradeTest extends FuryTestBase{
    @Test
    public void testLog(){
        Fury fury = Fury.builder()
                .exceptionLogMode(ExceptionLogMode.SAMPLE_PRINT, 2)
                .withLanguage(Language.JAVA)
                .withRefTracking(true)
                .build();
        fury.register(TestObj.class);
        fury.register(TestB.class);
//        TestObj testObj = builderTestObj();
//        byte[] serialize = fury.serialize(testObj);
//        String string = Base64.getEncoder().encodeToString(serialize);
//        System.out.println(string);
        String str = "AgDCAgDEAv8UdGVzdEL9/f3+Af4B/gH+Af4B/gH+Af3+AQ==";
        byte[] decode = Base64.getDecoder().decode(str);
        Object deserialize = fury.deserialize(decode);
        System.out.println(deserialize);
    }

    public TestObj builderTestObj(){
        TestObj testObj = new TestObj();
//        TestA testA = new TestA();
//        testA.setName("testA");
        TestB testB = new TestB();
        testB.setName("testB");
//        testObj.setTestA(testB);
//        testObj.setTest1(testB);
//        testObj.setTest2(testB);
//        testObj.setTest3(testB);
//        testObj.setTest4(testB);
//        testObj.setTest5(testB);
//        testObj.setTest6(testB);
//        testObj.setTest7(testB);
//        testObj.setTest8(testB);
        return testObj;
    }
}
