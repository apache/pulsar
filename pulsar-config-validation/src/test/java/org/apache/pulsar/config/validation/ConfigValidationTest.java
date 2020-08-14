/**
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
package org.apache.pulsar.config.validation;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class ConfigValidationTest {

    private final List<String> testStringList = Arrays.asList(new String[]{"foo", "bar"});
    private final List<Integer> testIntegerList = Arrays.asList(new Integer[]{0, 1});
    private final Map testStringIntegerMap = new HashMap<String, Integer>() {
        {
            put("one", 1);
            put("two", 2);
        }
    };
    private final Map testStringStringMap = new HashMap<String, String>() {
        {
            put("one", "one");
            put("two", "two");
        }
    };
    private final String topic = "persistent://public/default/topic";

    public static class TestValidator extends Validator {
        @Override
        public void validateField(String name, Object o) {
            if (o instanceof String) {
                String value = (String)o;
                if (!value.startsWith("ABCDE")) {
                    throw new IllegalArgumentException(String.format("Field %s does not start with ABCDE", name));
                }
            } else {
                throw new IllegalArgumentException(String.format("Field %s is not a string", name));
            }
        }
    }

    class TestConfig {
        @ConfigValidationAnnotations.NotNull
        public String stringValue;
        @ConfigValidationAnnotations.PositiveNumber
        public Integer positiveNumber;
        @ConfigValidationAnnotations.List(type = Integer.class)
        public List integerList;
        @ConfigValidationAnnotations.Map(keyType = String.class, valueType = Integer.class)
        public Map stringIntegerMap;
        @ConfigValidationAnnotations.StringList
        public List stringList;
        @ConfigValidationAnnotations.CustomType(validatorClass = TestValidator.class)
        public String customString;
    }

    @Test
    public void testGoodConfig() {
        TestConfig testConfig = createGoodConfig();
        ConfigValidation.validateConfig(testConfig);
    }

    @Test
    public void testNotNull() {
        TestConfig testConfig = createGoodConfig();
        testConfig.stringValue = null;
        Exception e = expectThrows(IllegalArgumentException.class, () -> ConfigValidation.validateConfig(testConfig));
        assertTrue(e.getMessage().contains("stringValue"));
    }

    @Test
    public void testPositiveNumber() {
        TestConfig testConfig = createGoodConfig();
        testConfig.positiveNumber = -2;
        Exception e = expectThrows(IllegalArgumentException.class, () -> ConfigValidation.validateConfig(testConfig));
        assertTrue(e.getMessage().contains("positiveNumber"));
    }

    @Test
    public void testListEntry() {
        TestConfig testConfig = createGoodConfig();
        testConfig.integerList = testStringList;
        Exception e = expectThrows(IllegalArgumentException.class, () -> ConfigValidation.validateConfig(testConfig));
        assertTrue(e.getMessage().contains("integerList"));
    }

    @Test
    public void testMapEntry() {
        TestConfig testConfig = createGoodConfig();
        testConfig.stringIntegerMap = testStringStringMap;
        Exception e = expectThrows(IllegalArgumentException.class, () -> ConfigValidation.validateConfig(testConfig));
        assertTrue(e.getMessage().contains("stringIntegerMap"));
    }

    @Test
    public void testStringList() {
        TestConfig testConfig = createGoodConfig();
        testConfig.stringList = testIntegerList;
        Exception e = expectThrows(IllegalArgumentException.class, () -> ConfigValidation.validateConfig(testConfig));
        assertTrue(e.getMessage().contains("stringList"));
    }

    @Test
    public void testCustomString() {
        TestConfig testConfig = createGoodConfig();
        testConfig.customString = "http://google.com";
        Exception e = expectThrows(IllegalArgumentException.class, () -> ConfigValidation.validateConfig(testConfig));
        assertTrue(e.getMessage().contains("customString"));
    }

    private TestConfig createGoodConfig() {
        TestConfig testConfig = new TestConfig();
        testConfig.stringValue = "string";
        testConfig.positiveNumber = 20;
        testConfig.integerList = testIntegerList;
        testConfig.stringIntegerMap = testStringIntegerMap;
        testConfig.stringList = testStringList;
        testConfig.customString = "ABCDEabcde";
        return testConfig;
    }
}
