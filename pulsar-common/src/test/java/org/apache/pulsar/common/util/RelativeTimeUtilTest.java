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
package org.apache.pulsar.common.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

public class RelativeTimeUtilTest {
    @Test
    public void testParseRelativeTime() {
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("-1"), -1);
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("7"), 7);
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("3s"), 3);
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("3S"), 3);
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("5m"), TimeUnit.MINUTES.toSeconds(5));
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("5M"), TimeUnit.MINUTES.toSeconds(5));
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("7h"), TimeUnit.HOURS.toSeconds(7));
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("7H"), TimeUnit.HOURS.toSeconds(7));
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("9d"), TimeUnit.DAYS.toSeconds(9));
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("9D"), TimeUnit.DAYS.toSeconds(9));
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("3w"), 7 * TimeUnit.DAYS.toSeconds(3));
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("11y"), 365 * TimeUnit.DAYS.toSeconds(11));
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("11Y"), 365 * TimeUnit.DAYS.toSeconds(11));

        // Negative interval
        assertEquals(RelativeTimeUtil.parseRelativeTimeInSeconds("-5m"), -TimeUnit.MINUTES.toSeconds(5));

        try {
            RelativeTimeUtil.parseRelativeTimeInSeconds("");
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            // Invalid time unit specified
            RelativeTimeUtil.parseRelativeTimeInSeconds("1234x");
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
