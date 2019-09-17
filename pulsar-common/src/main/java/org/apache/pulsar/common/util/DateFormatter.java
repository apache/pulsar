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

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Date-time String formatter utility class.
 */
public class DateFormatter {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME
            .withZone(ZoneId.systemDefault());

    /**
     * @return a String representing the current datetime
     */
    public static String now() {
        return format(Instant.now());
    }

    /**
     * @return a String representing a particular timestamp (in milliseconds)
     */
    public static String format(long timestamp) {
        return format(Instant.ofEpochMilli(timestamp));
    }

    /**
     * @return a String representing a particular time instant
     */
    public static String format(Instant instant) {
        return DATE_FORMAT.format(instant);
    }

    /**
     * @param datetime
     * @return the parsed timestamp (in milliseconds) of the provided datetime
     * @throws DateTimeParseException
     */
    public static long parse(String datetime) throws DateTimeParseException {
        Instant instant = Instant.from(DATE_FORMAT.parse(datetime));

        return instant.toEpochMilli();
    }

    private DateFormatter() {
    }
}
