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
package org.apache.pulsar.utils.auth.tokens;

import static org.junit.Assert.fail;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

public class TokenCliUtilsTest {
    @Test
    public void testParseRelativeTime() {
        assertEquals(TokensCliUtils.parseRelativeTimeInMillis("3s"), TimeUnit.SECONDS.toMillis(3));
        assertEquals(TokensCliUtils.parseRelativeTimeInMillis("5m"), TimeUnit.MINUTES.toMillis(5));
        assertEquals(TokensCliUtils.parseRelativeTimeInMillis("7h"), TimeUnit.HOURS.toMillis(7));
        assertEquals(TokensCliUtils.parseRelativeTimeInMillis("9d"), TimeUnit.DAYS.toMillis(9));
        assertEquals(TokensCliUtils.parseRelativeTimeInMillis("11y"), 365 * TimeUnit.DAYS.toMillis(11));

        // Negative interval
        assertEquals(TokensCliUtils.parseRelativeTimeInMillis("-5m"), -TimeUnit.MINUTES.toMillis(5));

        try {
            TokensCliUtils.parseRelativeTimeInMillis("");
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            // No time unit specified
            TokensCliUtils.parseRelativeTimeInMillis("1234");
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            // Invalid time unit specified
            TokensCliUtils.parseRelativeTimeInMillis("1234x");
            fail("should have failed");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }
}
