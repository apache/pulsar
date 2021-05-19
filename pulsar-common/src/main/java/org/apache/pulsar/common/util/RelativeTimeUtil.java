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

import java.util.concurrent.TimeUnit;
import lombok.experimental.UtilityClass;

/**
 * Parser for relative time.
 */
@UtilityClass
public class RelativeTimeUtil {
    public static long parseRelativeTimeInSeconds(String relativeTime) {
        if (relativeTime.isEmpty()) {
            throw new IllegalArgumentException("exipiry time cannot be empty");
        }

        int lastIndex =  relativeTime.length() - 1;
        char lastChar = relativeTime.charAt(lastIndex);
        final char timeUnit;

        if (!Character.isAlphabetic(lastChar)) {
            // No unit specified, assume seconds
            timeUnit = 's';
            lastIndex = relativeTime.length();
        } else {
            timeUnit = Character.toLowerCase(lastChar);
        }

        long duration = Long.parseLong(relativeTime.substring(0, lastIndex));

        switch (timeUnit) {
        case 's':
            return duration;
        case 'm':
            return TimeUnit.MINUTES.toSeconds(duration);
        case 'h':
            return TimeUnit.HOURS.toSeconds(duration);
        case 'd':
            return TimeUnit.DAYS.toSeconds(duration);
        case 'w':
            return 7 * TimeUnit.DAYS.toSeconds(duration);
        // No unit for months
        case 'y':
            return 365 * TimeUnit.DAYS.toSeconds(duration);
        default:
            throw new IllegalArgumentException("Invalid time unit '" + lastChar + "'");
        }
    }
}
