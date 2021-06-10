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

import com.google.common.collect.Lists;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.Nullable;
import org.apache.kafka.connect.data.Date;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.io.kafka.connect.schema.PulsarSchemaToKafkaSchema;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Test the conversion of PulsarSchema To KafkaSchema\.
 */
@Slf4j
public class PulsarSchemaToKafkaSchemaTest {

    static final List<String> STRUCT_FIELDS = Lists.newArrayList("field1", "field2", "field3");

    @Data
    static class StructWithAnnotations {
        int field1;
        @Nullable
        String field2;
        @AvroDefault("\"1000\"")
        Long field3;
    }

    @Test
    public void bytesSchemaTest() {
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.BYTES);
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.BYTES);

        kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.BYTEBUFFER);
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.BYTES);
    }

    @Test
    public void stringSchemaTest() {
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.STRING);
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.STRING);
    }

    @Test
    public void booleanSchemaTest() {
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.BOOL);
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.BOOLEAN);
    }

    @Test
    public void int8SchemaTest() {
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.INT8);
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.INT8);
    }

    @Test
    public void int16SchemaTest() {
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.INT16);
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.INT16);
    }

    @Test
    public void int32SchemaTest() {
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.INT32);
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.INT32);
    }

    @Test
    public void int64SchemaTest() {
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.INT64);
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.INT64);
    }

    @Test
    public void float32SchemaTest() {
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.FLOAT);
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.FLOAT32);
    }

    @Test
    public void float64SchemaTest() {
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.DOUBLE);
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.FLOAT64);
    }

    @Test
    public void kvBytesSchemaTest() {
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.KV_BYTES());
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.MAP);
        assertEquals(kafkaSchema.keySchema().type(), org.apache.kafka.connect.data.Schema.Type.BYTES);
        assertEquals(kafkaSchema.valueSchema().type(), org.apache.kafka.connect.data.Schema.Type.BYTES);
    }

    @Test
    public void kvBytesIntSchemaTests() {
        Schema pulsarKvSchema = KeyValueSchemaImpl.of(Schema.STRING, Schema.INT64);
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(pulsarKvSchema);
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.MAP);
        assertEquals(kafkaSchema.keySchema().type(), org.apache.kafka.connect.data.Schema.Type.STRING);
        assertEquals(kafkaSchema.valueSchema().type(), org.apache.kafka.connect.data.Schema.Type.INT64);
    }

    @Test
    public void avroSchemaTest() {
        AvroSchema<StructWithAnnotations> pulsarAvroSchema = AvroSchema.of(StructWithAnnotations.class);
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(pulsarAvroSchema);
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.STRUCT);
        assertEquals(kafkaSchema.fields().size(), STRUCT_FIELDS.size());
        for (String name: STRUCT_FIELDS) {
            assertEquals(kafkaSchema.field(name).name(), name);
        }
    }

    @Test
    public void jsonSchemaTest() {
        JSONSchema<StructWithAnnotations> jsonSchema = JSONSchema
                .of(SchemaDefinition.<StructWithAnnotations>builder()
                .withPojo(StructWithAnnotations.class)
                .withAlwaysAllowNull(false)
                .build());
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(jsonSchema);
        assertEquals(kafkaSchema.type(), org.apache.kafka.connect.data.Schema.Type.STRUCT);
        assertEquals(kafkaSchema.fields().size(), STRUCT_FIELDS.size());
        for (String name: STRUCT_FIELDS) {
            assertEquals(kafkaSchema.field(name).name(), name);
        }
    }

    @Test
    public void dateSchemaTest() {
        org.apache.kafka.connect.data.Schema kafkaSchema =
                PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.DATE);
        assertEquals(kafkaSchema.type(), Date.SCHEMA.type());
    }

    // not supported schemas below:
    @Test(expectedExceptions = IllegalStateException.class)
    public void timeSchemaTest() {
        PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.TIME);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void timestampSchemaTest() {
        PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.TIMESTAMP);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void instantSchemaTest() {
        PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.INSTANT);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void localDateSchemaTest() {
        PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.LOCAL_DATE);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void localTimeSchemaTest() {
        PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.LOCAL_TIME);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void localDatetimeSchemaTest() {
        PulsarSchemaToKafkaSchema.getKafkaConnectSchema(Schema.LOCAL_DATE_TIME);
    }

}
