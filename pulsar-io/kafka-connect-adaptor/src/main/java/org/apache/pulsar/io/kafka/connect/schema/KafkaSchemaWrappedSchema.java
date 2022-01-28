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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Wrapped schema for kafka connect schema.
 */
@Slf4j
public class KafkaSchemaWrappedSchema implements Schema<byte[]>, Serializable {

    private SchemaInfo schemaInfo = null;

    public KafkaSchemaWrappedSchema(org.apache.pulsar.kafka.shade.avro.Schema schema,
                                    Converter converter) {
        Map<String, String> props = new HashMap<>();
        boolean isJsonConverter = converter instanceof JsonConverter;
        props.put(GenericAvroSchema.OFFSET_PROP, isJsonConverter ? "0" : "5");
        this.schemaInfo = SchemaInfo.builder()
                .name(isJsonConverter ? "KafKaJson" : "KafkaAvro")
                .type(isJsonConverter ? SchemaType.JSON : SchemaType.AVRO)
                .schema(schema.toString().getBytes(StandardCharsets.UTF_8))
                .properties(props)
                .build();
    }

    @Override
    public byte[] encode(byte[] data) {
        return data;
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    @Override
    public Schema<byte[]> clone() {
        return null;
    }
}
