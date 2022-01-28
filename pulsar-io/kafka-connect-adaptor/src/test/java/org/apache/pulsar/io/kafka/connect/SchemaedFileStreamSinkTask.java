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

package org.apache.pulsar.io.kafka.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.file.FileStreamSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.testng.collections.Maps;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A FileStreamSinkTask for testing that writes data other than just a value, i.e.:
 * key, value, key and value schemas.
 */
public class SchemaedFileStreamSinkTask extends FileStreamSinkTask {

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {

        List<SinkRecord> out = Lists.newLinkedList();

        for (SinkRecord record: sinkRecords) {
            Object val = record.valueSchema() == Schema.BYTES_SCHEMA
                    ? new String((byte[]) record.value(), StandardCharsets.US_ASCII)
                    : record.value();

            Map<String, Object> recOut = Maps.newHashMap();
            recOut.put("keySchema", record.keySchema().type().toString());
            recOut.put("valueSchema", record.valueSchema().type().toString());
            recOut.put("key", record.key());
            if (val instanceof Struct) {
                Map<String, Object> map = Maps.newHashMap();
                Struct struct = (Struct)val;

                // no recursion needed for tests
                for (Field f: struct.schema().fields()) {
                    map.put(f.name(), struct.get(f));
                }

                recOut.put("value", map);
            } else {
                recOut.put("value", val);
            }

            ObjectMapper om = new ObjectMapper();
            try {
                SinkRecord toSink = new SinkRecord(record.topic(),
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        Schema.STRING_SCHEMA,
                        om.writeValueAsString(recOut),
                        record.kafkaOffset(),
                        record.timestamp(),
                        record.timestampType());
                out.add(toSink);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        super.put(out);
    }

}
