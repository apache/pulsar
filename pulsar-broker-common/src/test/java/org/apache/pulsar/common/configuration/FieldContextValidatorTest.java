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
package org.apache.pulsar.common.configuration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.lang.reflect.Field;
import java.util.Optional;
import org.testng.annotations.Test;

public class FieldContextValidatorTest {

    @Test
    public void testValidateStringSkipsWhenFieldHasNoAnnotation() throws Exception {
        Field field = TestConfig.class.getDeclaredField("noAnnotation");
        assertFalse(FieldContextValidator.validateString(field, "value").isPresent());
    }

    @Test
    public void testValidateStringAllowsBlankForOptionalField() throws Exception {
        Field field = TestConfig.class.getDeclaredField("optionalString");
        assertFalse(FieldContextValidator.validateString(field, "").isPresent());
        assertFalse(FieldContextValidator.validateString(field, "   ").isPresent());
        assertFalse(FieldContextValidator.validateString(field, null).isPresent());
    }

    @Test
    public void testValidateStringRejectsBlankForRequiredField() throws Exception {
        Field field = TestConfig.class.getDeclaredField("requiredString");
        Optional<String> error = FieldContextValidator.validateString(field, "");
        assertTrue(error.isPresent());
        assertEquals(error.get(), "Required requiredString is null");
    }

    @Test
    public void testValidateStringAcceptsValidBooleanValues() throws Exception {
        Field field = TestConfig.class.getDeclaredField("enabled");
        assertFalse(FieldContextValidator.validateString(field, "true").isPresent());
        assertFalse(FieldContextValidator.validateString(field, "false").isPresent());
        assertFalse(FieldContextValidator.validateString(field, "TRUE").isPresent());
    }

    @Test
    public void testValidateStringRejectsInvalidBooleanValues() throws Exception {
        Field field = TestConfig.class.getDeclaredField("enabled");
        Optional<String> error = FieldContextValidator.validateString(field, "abc");
        assertTrue(error.isPresent());
        assertEquals(error.get(), "enabled must be 'true' or 'false'");
    }

    @Test
    public void testValidateStringRejectsUnparseableNumericValue() throws Exception {
        Field field = TestConfig.class.getDeclaredField("boundedInt");
        Optional<String> error = FieldContextValidator.validateString(field, "abc");
        assertTrue(error.isPresent());
        assertTrue(error.get().startsWith("Failed to parse boundedInt:"));
    }

    @Test
    public void testValidateStringRejectsOutOfRangeNumericValue() throws Exception {
        Field field = TestConfig.class.getDeclaredField("boundedInt");
        Optional<String> error = FieldContextValidator.validateString(field, "0");
        assertTrue(error.isPresent());
        assertEquals(error.get(), "boundedInt value 0 doesn't fit in given range (1, 3)");
    }

    @Test
    public void testValidateStringAcceptsValidNumericValue() throws Exception {
        Field field = TestConfig.class.getDeclaredField("boundedInt");
        assertFalse(FieldContextValidator.validateString(field, "2").isPresent());
    }

    @Test
    public void testValidateStringRejectsStringExceedingMaxCharLength() throws Exception {
        Field field = TestConfig.class.getDeclaredField("shortString");
        Optional<String> error = FieldContextValidator.validateString(field, "abcd");
        assertTrue(error.isPresent());
        assertEquals(error.get(), "shortString exceeds maxCharLength 3");
    }

    @Test
    public void testValidateParsedValueSkipsWhenFieldHasNoAnnotation() throws Exception {
        Field field = TestConfig.class.getDeclaredField("noAnnotation");
        assertFalse(FieldContextValidator.validateParsedValue(field, "value").isPresent());
    }

    @Test
    public void testValidateParsedValueRejectsRequiredNull() throws Exception {
        Field field = TestConfig.class.getDeclaredField("requiredString");
        Optional<String> error = FieldContextValidator.validateParsedValue(field, null);
        assertTrue(error.isPresent());
        assertEquals(error.get(), "Required requiredString is null");
    }

    @Test
    public void testValidateParsedValueRejectsRequiredBlankString() throws Exception {
        Field field = TestConfig.class.getDeclaredField("requiredString");
        Optional<String> error = FieldContextValidator.validateParsedValue(field, "  ");
        assertTrue(error.isPresent());
        assertEquals(error.get(), "Required requiredString is null");
    }

    @Test
    public void testValidateParsedValueRejectsOutOfRangeNumber() throws Exception {
        Field field = TestConfig.class.getDeclaredField("boundedInt");
        Optional<String> error = FieldContextValidator.validateParsedValue(field, 4);
        assertTrue(error.isPresent());
        assertEquals(error.get(), "boundedInt value 4 doesn't fit in given range (1, 3)");
    }

    @Test
    public void testValidateParsedValueAcceptsInRangeNumber() throws Exception {
        Field field = TestConfig.class.getDeclaredField("boundedInt");
        assertFalse(FieldContextValidator.validateParsedValue(field, 2).isPresent());
    }

    @Test
    public void testValidateParsedValueRejectsStringExceedingMaxCharLength() throws Exception {
        Field field = TestConfig.class.getDeclaredField("shortString");
        Optional<String> error = FieldContextValidator.validateParsedValue(field, "abcd");
        assertTrue(error.isPresent());
        assertEquals(error.get(), "shortString exceeds maxCharLength 3");
    }

    static class TestConfig {
        String noAnnotation;

        @FieldContext(required = true)
        String requiredString;

        @FieldContext
        String optionalString;

        @FieldContext(dynamic = true)
        boolean enabled;

        @FieldContext(minValue = 1, maxValue = 3)
        int boundedInt;

        @FieldContext(maxCharLength = 3)
        String shortString;
    }
}
