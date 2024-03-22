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
package org.apache.pulsar.cli.converters;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import org.apache.pulsar.cli.converters.picocli.ByteUnitToIntegerConverter;
import org.apache.pulsar.cli.converters.picocli.ByteUnitToLongConverter;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import picocli.CommandLine.TypeConversionException;

public class ByteConversionTest {

    @DataProvider
    public static Object[][] successfulByteUnitUtilTestCases() {
        return new Object[][] {
                {"4096", 4096L},
                {"1000", 1000L},
                {"100K", 102400L},
                {"100k", 102400L},
                {"100M", 104857600L},
                {"100m", 104857600L},
                {"100G", 107374182400L},
                {"100g", 107374182400L},
                {"100T", 109951162777600L},
                {"100t", 109951162777600L},
        };
    }

    @DataProvider
    public static Object[][] failingByteUnitUtilTestCases() {
        return new Object[][] {
                {""}, // Empty string
                {"1Z"}, // Invalid size unit
                {"1.5K"}, // Non-integer value
                {"K"} // Missing size value
        };
    }

    @Test(dataProvider = "successfulByteUnitUtilTestCases")
    public void testSuccessfulByteUnitUtilConversion(String input, long expected) {
        assertEquals(ByteUnitUtil.validateSizeString(input), expected);
    }

    @Test(dataProvider = "successfulByteUnitUtilTestCases")
    public void testSuccessfulByteUnitToLongConverter(String input, long expected) throws Exception{
        ByteUnitToLongConverter converter = new ByteUnitToLongConverter();
        assertEquals(converter.convert(input), Long.valueOf(expected));
    }

    @Test(dataProvider = "successfulByteUnitUtilTestCases")
    public void testSuccessfulByteUnitIntegerConverter(String input, long expected) throws Exception {
        ByteUnitToIntegerConverter converter = new ByteUnitToIntegerConverter();
        // Since the converter returns an Integer, we need to cast expected to int
        assertEquals(converter.convert(input), Integer.valueOf((int) expected));
    }

    @Test(dataProvider = "failingByteUnitUtilTestCases")
    public void testFailedByteUnitUtilConversion(String input) {
        assertThrows(IllegalArgumentException.class, () -> ByteUnitUtil.validateSizeString(input));
    }

    @Test(dataProvider = "failingByteUnitUtilTestCases")
    public void testFailedByteUnitToLongConverter(String input) {
        ByteUnitToLongConverter converter = new ByteUnitToLongConverter();
        assertThrows(TypeConversionException.class, () -> converter.convert(input));
    }

    @Test(dataProvider = "failingByteUnitUtilTestCases")
    public void testFailedByteUnitIntegerConverter(String input) {
        ByteUnitToIntegerConverter converter = new ByteUnitToIntegerConverter();
        assertThrows(TypeConversionException.class, () -> converter.convert(input));
    }
}

