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
package org.apache.pulsar.io.kafka.connect.schema;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.kafka.shade.io.confluent.connect.avro.AvroConverter;
import org.apache.pulsar.kafka.shade.io.confluent.connect.avro.AvroData;
import org.apache.pulsar.kafka.shade.io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.pulsar.kafka.shade.io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@Slf4j
public class KafkaSchemaTest {

    private final KafkaSchema kafkaSchema;
    private final AvroData avroData;
    private final Schema<User> userJsonSchema = Schema.JSON(
            SchemaDefinition.<User>builder()
                    .withPojo(User.class)
                    .withAlwaysAllowNull(false)
                    .withSupportSchemaVersioning(true)
                    .build());

    public KafkaSchemaTest() {
        kafkaSchema = new KafkaSchema();
        avroData = new AvroData(1000);
    }

    @Before
    public void setup() {
    }

    /**
     * User class.
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class User {
        String name;
        int age;
    }

    @Test
    public void testJsonConverter() throws Exception {
        String AVRO_USER_SCHEMA_DEF = "{" +
                "\"type\":\"record\"," +
                "\"name\":\"User\"," +
                "\"namespace\":\"org.apache.pulsar.io.kafka.connect.schema.KafkaSchemaTest\"," +
                "\"fields\":[" +
                "{\"name\":\"name\",\"type\":\"string\",\"default\":null}," +
                "{\"name\":\"age\",\"type\":\"int\"}" +
                "]}";
        org.apache.pulsar.kafka.shade.avro.Schema AVRO_USER_SCHEMA = new org.apache.pulsar.kafka.shade.avro.Schema
                .Parser().parse(AVRO_USER_SCHEMA_DEF);
        kafkaSchema.setAvroSchema(
                false,
                avroData,
                AVRO_USER_SCHEMA,
                new JsonConverter()
        );

        User user = new User("user-1", 100);
        byte[] data = userJsonSchema.encode(user);
        byte[] encodedData = kafkaSchema.encode(data);
        assertSame(data, encodedData);
    }

    @Test
    public void testAvroConverter() throws Exception {
        String AVRO_USER_SCHEMA_DEF = "{" +
                "\"type\":\"record\"," +
                "\"name\":\"User\"," +
                "\"namespace\":\"org.apache.pulsar.io.kafka.connect.schema.KafkaSchemaTest\"," +
                "\"fields\":[" +
                "{\"name\":\"name\",\"type\":\"string\"}," +
                "{\"name\":\"age\",\"type\":\"int\"}" +
                "]}";
        org.apache.pulsar.kafka.shade.avro.Schema AVRO_USER_SCHEMA = new org.apache.pulsar.kafka.shade.avro.Schema
                .Parser().parse(AVRO_USER_SCHEMA_DEF);
        Map<String, Object> config = new HashMap<>();
        AvroConverter converter = new AvroConverter(new MockSchemaRegistryClient());
        config.put(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "mock");
        converter.configure(config, false);
        kafkaSchema.setAvroSchema(
                false,
                avroData,
                AVRO_USER_SCHEMA,
                converter
        );

        log.info("Initialized with avro schema {}", AVRO_USER_SCHEMA);

        User user = new User("user-1", 100);
        byte[] jsonData = userJsonSchema.encode(user);

        byte[] avroData = kafkaSchema.encode(jsonData);


        Schema<GenericRecord> userAvroSchema = Schema.generic(
                SchemaInfo.builder()
                        .name("")
                        .properties(Collections.emptyMap())
                        .type(SchemaType.AVRO)
                        .schema(userJsonSchema.getSchemaInfo().getSchema())
                        .build()
        );
        GenericRecord userRecord = userAvroSchema.decode(avroData);
        assertEquals(user.getName(), userRecord.getField("name"));
        assertEquals(user.getAge(), userRecord.getField("age"));

    }
}

