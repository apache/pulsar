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
package org.apache.pulsar.io.alluxio.sink;

import com.google.common.collect.Lists;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.SinkRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

public class SinkRecordHelper {

    private static final String TOPIC = "fake_topic_name";

    private static Record<GenericObject> build(String topic, String key, String value) {
        // prepare a SinkRecord
        SinkRecord<GenericObject> record = new SinkRecord(new Record<GenericObject>() {

            @Override
            public GenericObject getValue() {
                return new GenericObject() {
                    @Override
                    public SchemaType getSchemaType() {
                        return SchemaType.BYTES;
                    }

                    @Override
                    public Object getNativeObject() {
                        return key.getBytes(StandardCharsets.UTF_8);
                    }
                };
            }

            @Override
            public Optional<String> getDestinationTopic() {
                if (topic != null) {
                    return Optional.of(topic);
                } else {
                    return Optional.empty();
                }
            }
        }, value);
        return record;
    }

    public static List<Record<GenericObject>> buildBatch(int size) {
        List<Record<GenericObject>> records = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            Record<GenericObject> record = build(TOPIC, "FakeKey" + i, "FakeValue" + i);
            records.add(record);
        }
        return records;
    }

}
