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
import org.testng.annotations.Test;

public class ValueValidationUtilTest {

    @Test
    public void testMaxValueCheck() {
        assertThrows(IllegalArgumentException.class, () -> ValueValidationUtil.maxValueCheck("param1", 11L, 10L));
        ValueValidationUtil.maxValueCheck("param2", 10L, 10L);
        ValueValidationUtil.maxValueCheck("param3", 9L, 10L);
    }

    @Test
    public void testPositiveCheck() {
        // Long
        assertThrows(IllegalArgumentException.class, () -> ValueValidationUtil.positiveCheck("param1", 0L));
        assertThrows(IllegalArgumentException.class, () -> ValueValidationUtil.positiveCheck("param2", -1L));
        ValueValidationUtil.positiveCheck("param3", 1L);

        // Integer
        assertThrows(IllegalArgumentException.class, () -> ValueValidationUtil.positiveCheck("param4", 0));
        assertThrows(IllegalArgumentException.class, () -> ValueValidationUtil.positiveCheck("param5", -1));
        ValueValidationUtil.positiveCheck("param6", 1);
    }

    @Test
    public void testEmptyCheck() {
        assertThrows(IllegalArgumentException.class, () -> ValueValidationUtil.emptyCheck("param1", ""));
        assertThrows(IllegalArgumentException.class, () -> ValueValidationUtil.emptyCheck("param2", null));
        ValueValidationUtil.emptyCheck("param3", "nonEmpty");
    }

    @Test
    public void testMinValueCheck() {
        assertThrows(IllegalArgumentException.class, () -> ValueValidationUtil.minValueCheck("param1", 9L, 10L));
        ValueValidationUtil.minValueCheck("param2", 10L, 10L);
        ValueValidationUtil.minValueCheck("param3", 11L, 10L);
    }

    @Test
    public void testPositiveCheckInt() {
        assertThrows(IllegalArgumentException.class, () -> ValueValidationUtil.positiveCheck("param1", 0));
        assertThrows(IllegalArgumentException.class, () -> ValueValidationUtil.positiveCheck("param2", -1));
        ValueValidationUtil.positiveCheck("param3", 1);
    }
}
