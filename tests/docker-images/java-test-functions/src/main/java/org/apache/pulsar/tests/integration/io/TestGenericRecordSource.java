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
package org.apache.pulsar.tests.integration.io;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TestGenericRecordSource implements Source<GenericRecord> {

    private Schema<GenericRecord> schema1;
    private List<Field> fields;
    private AtomicInteger count = new AtomicInteger();

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        RecordSchemaBuilder builder = SchemaBuilder.record("MyBean");
        builder.field("number").type(SchemaType.INT32);
        builder.field("text").type(SchemaType.STRING);
        schema1 = Schema.generic(builder.build(SchemaType.AVRO));
        fields = Arrays.asList(new Field("number", 0),
                new Field("text", 1));
        log.info("created source, schema {}", new String(schema1.getSchemaInfo().getSchema(), StandardCharsets.UTF_8));
    }

    @Override
    public Record<GenericRecord> read() throws Exception {
        // slow down the production of values
        Thread.sleep(20);

        Map<String, Object> values = new HashMap<>();
        int value = count.incrementAndGet();
        values.put("number", value);
        values.put("text", (value + 1) + "");
        GenericRecord record = new GenericRecord() {
            @Override
            public byte[] getSchemaVersion() {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<Field> getFields() {
                return fields;
            }

            @Override
            public Object getField(String s) {
                return values.get(s);
            }

            @Override
            public String toString() {
                return "SyntheticRecord " + values;
            }
        };
        log.info("produced {}", record);
        return new Record<GenericRecord>() {
            @Override
            public GenericRecord getValue() {
                return record;
            }

            @Override
            public Schema<GenericRecord> getSchema() {
                return schema1;
            }
        };
    }

    @Override
    public void close() throws Exception {

    }
}