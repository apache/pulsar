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
package org.apache.pulsar.cli;

import static org.testng.Assert.assertThrows;
import com.beust.jcommander.ParameterException;
import org.testng.annotations.Test;

public class ValueValidationUtilsTest {

    @Test
    public void testMaxValueCheck() {
        assertThrows(ParameterException.class, () -> ValueValidationUtils.maxValueCheck("param1", 11L, 10L));
        ValueValidationUtils.maxValueCheck("param2", 10L, 10L);
        ValueValidationUtils.maxValueCheck("param3", 9L, 10L);
    }

    @Test
    public void testPositiveCheck() {
        // Long
        assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck("param1", 0L));
        assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck("param2", -1L));
        ValueValidationUtils.positiveCheck("param3", 1L);

        // Integer
        assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck("param4", 0));
        assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck("param5", -1));
        ValueValidationUtils.positiveCheck("param6", 1);
    }

    @Test
    public void testEmptyCheck() {
        assertThrows(ParameterException.class, () -> ValueValidationUtils.emptyCheck("param1", ""));
        assertThrows(ParameterException.class, () -> ValueValidationUtils.emptyCheck("param2", null));
        ValueValidationUtils.emptyCheck("param3", "nonEmpty");
    }

    @Test
    public void testMinValueCheck() {
        assertThrows(ParameterException.class, () -> ValueValidationUtils.minValueCheck("param1", 9L, 10L));
        ValueValidationUtils.minValueCheck("param2", 10L, 10L);
        ValueValidationUtils.minValueCheck("param3", 11L, 10L);
    }

    @Test
    public void testPositiveCheckInt() {
        assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck("param1", 0));
        assertThrows(ParameterException.class, () -> ValueValidationUtils.positiveCheck("param2", -1));
        ValueValidationUtils.positiveCheck("param3", 1);
    }
}
