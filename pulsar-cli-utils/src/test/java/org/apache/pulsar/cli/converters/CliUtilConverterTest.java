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
import com.beust.jcommander.ParameterException;
import org.testng.annotations.Test;

public class CliUtilConverterTest {

    @Test
    public void testTimeUnitToSecondsConverter() {
        TimeUnitToSecondsConverter converter = new TimeUnitToSecondsConverter("optionName");
        assertEquals(converter.convert("10s"), Long.valueOf(10));
        assertEquals(converter.convert("1m"), Long.valueOf(60));
        assertThrows(ParameterException.class, () -> converter.convert(null));
        assertThrows(ParameterException.class, () -> converter.convert(""));
    }

    @Test
    public void testTimeUnitToMillisConverter() {
        TimeUnitToMillisConverter converter = new TimeUnitToMillisConverter("optionName");
        assertEquals(converter.convert("1s"), Long.valueOf(1000));
        assertEquals(converter.convert("1m"), Long.valueOf(60000));
        assertThrows(ParameterException.class, () -> converter.convert(null));
        assertThrows(ParameterException.class, () -> converter.convert(""));
    }

    @Test
    public void testByteUnitToLongConverter() {
        ByteUnitToLongConverter converter = new ByteUnitToLongConverter("optionName");
        assertEquals(converter.convert("1"), Long.valueOf(1));
        assertEquals(converter.convert("1K"), Long.valueOf(1024));
        assertThrows(ParameterException.class, () -> converter.convert(null));
        assertThrows(ParameterException.class, () -> converter.convert(""));
    }

    @Test
    public void testByteUnitIntegerConverter() {
        ByteUnitIntegerConverter converter = new ByteUnitIntegerConverter("optionName");
        assertEquals(converter.convert("1"), Integer.valueOf(1));
        assertEquals(converter.convert("1K"), Integer.valueOf(1024));
        assertThrows(ParameterException.class, () -> converter.convert(null));
        assertThrows(ParameterException.class, () -> converter.convert(""));
    }
}
