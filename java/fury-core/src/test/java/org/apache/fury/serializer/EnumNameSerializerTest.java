package org.apache.fury.serializer;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.codegen.JaninoUtils;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class EnumNameSerializerTest extends FuryTestBase {

    public enum EnumFoo {
        A,
        B
    }

    public enum EnumSubClass {
        A {
            @Override
            void f() {}
        },
        B {
            @Override
            void f() {}
        };

        abstract void f();
    }

    @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
    public void testEnumSerialization(boolean referenceTracking, Language language) {
        FuryBuilder builder =
                Fury.builder()
                        .withLanguage(language)
                        .withRefTracking(referenceTracking)
                        .withSerializeEnumByName(true)
                        .requireClassRegistration(false);
        Fury fury1 = builder.build();
        Fury fury2 = builder.build();
        assertEquals(EnumSerializerTest.EnumFoo.A, serDe(fury1, fury2, EnumSerializerTest.EnumFoo.A));
        assertEquals(EnumSerializerTest.EnumFoo.B, serDe(fury1, fury2, EnumSerializerTest.EnumFoo.B));
        assertEquals(EnumSerializerTest.EnumSubClass.A, serDe(fury1, fury2, EnumSerializerTest.EnumSubClass.A));
        assertEquals(EnumSerializerTest.EnumSubClass.B, serDe(fury1, fury2, EnumSerializerTest.EnumSubClass.B));
    }
    @Test
    public void testEnumSerializationUnexistentEnumValueAsNull() {
        String enumCode2 = "enum TestEnum2 {" + " A;" + "}";
        String enumCode1 = "enum TestEnum2 {" + " A, B" + "}";
        Class<?> cls1 =
                JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum2", enumCode1);
        Class<?> cls2 =
                JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum2", enumCode2);
        FuryBuilder builderSerialization =
                Fury.builder()
                        .withLanguage(Language.JAVA)
                        .withRefTracking(true)
                        .withSerializeEnumByName(true)
                        .requireClassRegistration(false);
        FuryBuilder builderDeserialize =
                Fury.builder()
                        .withLanguage(Language.JAVA)
                        .withRefTracking(true)
                        .requireClassRegistration(false)
                        .withSerializeEnumByName(true)
                        .deserializeNonexistentEnumValueAsNull(true)
                        .withClassLoader(cls2.getClassLoader());
        Fury furyDeserialize = builderDeserialize.build();
        Fury furySerialization = builderSerialization.build();
        byte[] bytes = furySerialization.serialize(cls1.getEnumConstants()[1]);
        Object data = furyDeserialize.deserialize(bytes);
    }

    @Data
    @AllArgsConstructor
    static class EnumSubclassFieldTest {
        EnumSubClass subEnum;
    }

    @Test(dataProvider = "enableCodegen")
    public void testEnumSubclassField(boolean enableCodegen) {
        serDeCheck(
                builder().withCodegen(enableCodegen).build(), new EnumSubclassFieldTest(EnumSubClass.B));
    }

    @Test(dataProvider = "scopedMetaShare")
    public void testEnumSubclassFieldCompatible(boolean scopedMetaShare) {
        serDeCheck(
                builder()
                        .withScopedMetaShare(scopedMetaShare)
                        .withCompatibleMode(CompatibleMode.COMPATIBLE)
                        .withSerializeEnumByName(true)
                        .build(),
                new EnumSerializerTest.EnumSubclassFieldTest(EnumSerializerTest.EnumSubClass.B));
    }
}
