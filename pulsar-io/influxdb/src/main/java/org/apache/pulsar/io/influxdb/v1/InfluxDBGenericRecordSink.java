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
package org.apache.pulsar.io.influxdb.v1;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.influxdb.dto.Point;

/**
 * A Simple InfluxDB sink, which interprets input Record in generic record.
 * In order to successfully parse and write points to InfluxDB, points must be in InfluxDB’s Line Protocol format.
 * This class expects records from Pulsar to have a field named 'measurement', a field named 'tags' if necessary.
 */
@Slf4j
public class InfluxDBGenericRecordSink extends InfluxDBAbstractSink<GenericRecord> {

    private static final Set<String> FIELDS_TO_SKIP = ImmutableSet.of("measurement", "tags");

    @Override
    protected Point buildPoint(Record<GenericRecord> message) throws Exception {
        Map<String, String> tags;
        Map<String, Object> fields = Maps.newHashMap();

        GenericRecord record = message.getValue();

        Object measurementField = getFiled(record, "measurement");
        if (null == measurementField) {
            throw new SchemaSerializationException("measurement is a required field.");
        }

        String measurement = measurementField.toString();

        // Looking for tags
        Object tagsField = getFiled(record, "tags");
        if (null == tagsField) {
            tags = ImmutableMap.of();
        } else if (Map.class.isAssignableFrom(tagsField.getClass())) {
            tags = ((Map<Object, Object>) tagsField).entrySet()
                    .stream().collect(Collectors.toMap(
                            entry -> entry.getKey().toString(),
                            entry -> entry.getValue().toString())
                    );
        } else {
            // Field 'tags' that is not of Map type will be ignored
            tags = ImmutableMap.of();
        }

        // Just insert the current time millis
        long timestamp = System.currentTimeMillis();

        for (Field field : record.getFields()) {
            String fieldName = field.getName();
            if (FIELDS_TO_SKIP.contains(fieldName)) {
                continue;
            }
            Object fieldValue = record.getField(field);
            if (null != fieldValue) {
                fields.put(fieldName, fieldValue);
            }
        }

        Point.Builder builder = Point.measurement(measurement)
                .time(timestamp, TimeUnit.MILLISECONDS)
                .tag(tags)
                .fields(fields);
        return builder.build();
    }
}
