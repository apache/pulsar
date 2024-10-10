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
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.cli.converters.picocli.TimeUnitToMillisConverter;
import org.apache.pulsar.cli.converters.picocli.TimeUnitToSecondsConverter;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TimeConversionTest {

    @DataProvider
    public static Object[][] successfulRelativeTimeUtilTestCases() {
        return new Object[][] {
                {"-1", -1L},
                {"7", 7L},
                {"100", 100L}, // No time unit, assuming seconds
                {"3s", 3L},
                {"3S", 3L},
                {"10s", 10L},
                {"1m", 60L},
                {"5m", TimeUnit.MINUTES.toSeconds(5L)},
                {"5M", TimeUnit.MINUTES.toSeconds(5L)},
                {"7h", TimeUnit.HOURS.toSeconds(7L)},
                {"7H", TimeUnit.HOURS.toSeconds(7L)},
                {"9d", TimeUnit.DAYS.toSeconds(9L)},
                {"9D", TimeUnit.DAYS.toSeconds(9L)},
                {"1w", 604800L},
                {"3W", 7 * TimeUnit.DAYS.toSeconds(3L)},
                {"11y", 365 * TimeUnit.DAYS.toSeconds(11L)},
                {"11Y", 365 * TimeUnit.DAYS.toSeconds(11L)},
                {"-5m", -TimeUnit.MINUTES.toSeconds(5L)}
        };
    }

    @Test(dataProvider = "successfulRelativeTimeUtilTestCases")
    public void testSuccessfulRelativeTimeUtilParsing(String input, long expected) {
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds(input), expected);
    }

    @Test(dataProvider = "successfulRelativeTimeUtilTestCases")
    public void testSuccessfulTimeUnitToSecondsConverter(String input, long expected) throws Exception {
        TimeUnitToSecondsConverter secondsConverter = new TimeUnitToSecondsConverter();
        assertEquals(secondsConverter.convert(input), Long.valueOf(expected));
    }

    @Test(dataProvider = "successfulRelativeTimeUtilTestCases")
    public void testSuccessfulTimeUnitToMillisConverter(String input, long expected) throws Exception {
        TimeUnitToMillisConverter millisConverter = new TimeUnitToMillisConverter();
        // We multiply the expected by 1000 to convert the seconds into milliseconds
        assertEquals(millisConverter.convert(input), Long.valueOf(expected * 1000));
    }

    @Test
    public void testFailingParsing() {
        assertThrows(IllegalArgumentException.class, () -> RelativeTimeUtil.parseRelativeTimeInSeconds("")); // Empty string
        assertThrows(IllegalArgumentException.class, () -> RelativeTimeUtil.parseRelativeTimeInSeconds("s")); // Non-numeric character
        assertThrows(IllegalArgumentException.class, () -> RelativeTimeUtil.parseRelativeTimeInSeconds("1z")); // Invalid time unit
        assertThrows(IllegalArgumentException.class, () -> RelativeTimeUtil.parseRelativeTimeInSeconds("1.5")); // Floating point number
    }

    @Test
    public void testNsToSeconds() {
        assertEquals(RelativeTimeUtil.nsToSeconds(1_000_000_000), 1.000);
        assertEquals(RelativeTimeUtil.nsToSeconds(1_500_000_000), 1.500);
        assertEquals(RelativeTimeUtil.nsToSeconds(1_555_555_555), 1.556);
    }
}
