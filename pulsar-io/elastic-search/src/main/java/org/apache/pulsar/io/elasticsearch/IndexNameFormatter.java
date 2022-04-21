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
package org.apache.pulsar.io.elasticsearch;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;

/**
 * A class that helps to generate the time-based index names.
 *
 * The formatter finds the pattern %{+<date-format>} in the format string
 * and replace them with strings that are formatted from the event time of the record
 * using <date-format>. The format follows the rules of {@link java.time.format.DateTimeFormatter}.
 *
 * For example, suppose the event time of the record is 1645182000000L, the indexName
 * is "logs-%{+yyyy-MM-dd}", then the formatted index name would be "logs-2022-02-18".
 */
public class IndexNameFormatter {
    private static final Pattern PATTERN_FIELD_REF = Pattern.compile("%\\{\\+(.+?)}");

    private final String indexNameFormat;
    private final List<String> segments;
    private final List<DateTimeFormatter> dateTimeFormatters;

    public IndexNameFormatter(String indexNameFormat) {
        this.indexNameFormat = indexNameFormat;

        Pair<List<String>, List<DateTimeFormatter>> parsed = parseFormat(indexNameFormat);
        this.segments = parsed.getKey();
        this.dateTimeFormatters = parsed.getRight();
    }

    static Pair<List<String>, List<DateTimeFormatter>> parseFormat(String format) {
        List<String> segments = new ArrayList<>();
        List<DateTimeFormatter> formatters = new ArrayList<>();
        Matcher matcher = PATTERN_FIELD_REF.matcher(format);
        int pos = 0;
        while (matcher.find()) {
            segments.add(format.substring(pos, matcher.start()));
            formatters.add(DateTimeFormatter.ofPattern(matcher.group(1))
                    .withLocale(Locale.ENGLISH)
                    .withZone(ZoneId.of("UTC")));
            pos = matcher.end();
        }
        segments.add(format.substring(pos));
        return Pair.of(segments, formatters);
    }

    static void validate(String format) {
        Pair<List<String>, List<DateTimeFormatter>> parsed = parseFormat(format);
        for (String s : parsed.getLeft()) {
            if (!s.toLowerCase(Locale.ROOT).equals(s)) {
                throw new IllegalArgumentException("indexName should be lowercase only.");
            }
        }
    }

    public String indexName(Record<GenericObject> record) {
        if (this.dateTimeFormatters.isEmpty()) {
            return this.indexNameFormat;
        }
        Instant eventTime = Instant.ofEpochMilli(record.getEventTime()
                .orElseThrow(() -> new IllegalStateException("No event time in record")));
        StringBuilder builder = new StringBuilder(this.segments.get(0));

        for (int i = 0; i < dateTimeFormatters.size(); i++) {
            builder.append(dateTimeFormatters.get(i).format(eventTime));
            builder.append(this.segments.get(i + 1));
        }

        return builder.toString();
    }
}
