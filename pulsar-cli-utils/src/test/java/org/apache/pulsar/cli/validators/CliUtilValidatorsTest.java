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
package org.apache.pulsar.cli.validators;

import static org.testng.Assert.assertThrows;
import org.testng.annotations.Test;

public class CliUtilValidatorsTest {

    @Test
    public void testPositiveLongValueValidator() {
        PositiveLongValueValidator validator = new PositiveLongValueValidator();
        assertThrows(IllegalArgumentException.class, () -> validator.validate("param", -1L));
        assertThrows(IllegalArgumentException.class, () -> validator.validate("param", 0L));
        validator.validate("param", 1L);
    }

    @Test
    public void testPositiveIntegerValueValidator() {
        PositiveIntegerValueValidator validator = new PositiveIntegerValueValidator();
        assertThrows(IllegalArgumentException.class, () -> validator.validate("param", -1));
        assertThrows(IllegalArgumentException.class, () -> validator.validate("param", 0));
        validator.validate("param", 1);
    }

    @Test
    public void testNonNegativeValueValidator() {
        NonNegativeValueValidator validator = new NonNegativeValueValidator();
        assertThrows(IllegalArgumentException.class, () -> validator.validate("param", -1L));
        validator.validate("param", 0L);
        validator.validate("param", 1L);
    }

    @Test
    public void testMinNegativeOneValidator() {
        MinNegativeOneValidator validator = new MinNegativeOneValidator();
        assertThrows(IllegalArgumentException.class, () -> validator.validate("param", -2L));
        validator.validate("param", -1L);
        validator.validate("param", 0L);
    }

    @Test
    public void testIntegerMaxValueLongValidator() {
        IntegerMaxValueLongValidator validator = new IntegerMaxValueLongValidator();
        assertThrows(IllegalArgumentException.class, () -> validator.validate("param", Integer.MAX_VALUE + 1L));
        validator.validate("param", (long) Integer.MAX_VALUE);
    }
}
