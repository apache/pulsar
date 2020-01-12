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

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pulsar.kafka.shade.io.confluent.connect.avro.AvroConverter;
import org.apache.pulsar.kafka.shade.io.confluent.connect.avro.AvroData;
import org.apache.pulsar.kafka.shade.avro.generic.GenericDatumWriter;
import org.apache.pulsar.kafka.shade.avro.generic.GenericRecord;
import org.apache.pulsar.kafka.shade.avro.io.BinaryEncoder;
import org.apache.pulsar.kafka.shade.avro.io.EncoderFactory;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.storage.Converter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

import static java.nio.charset.StandardCharsets.UTF_8;

public class KafkaSchema implements Schema<byte[]> {

    private AvroData avroData = null;
    private final JsonDeserializer jsonDeserializer = new JsonDeserializer();
    private Converter valueConverter = null;
    private SchemaInfo schemaInfo = null;
    private org.apache.pulsar.kafka.shade.avro.Schema avroSchema = null;
    private final Method convertToConnectMethod;

    public KafkaSchema() {
        try {
            this.convertToConnectMethod = JsonConverter.class.getDeclaredMethod(
                    "convertToConnect",
                    org.apache.kafka.connect.data.Schema.class,
                    JsonNode.class
            );
            this.convertToConnectMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to locate `convertToConnect` method for JsonConverter", e);
        }
    }

    public void setAvroSchema(boolean isKey,
                              AvroData avroData,
                              org.apache.pulsar.kafka.shade.avro.Schema schema,
                              Converter converter) {
        this.valueConverter = converter;
        this.avroData = avroData;
        this.avroSchema = schema;
        this.schemaInfo = SchemaInfo.builder()
                .name(converter instanceof JsonConverter ? "KafkaJson" : "KafkaAvro")
                .type(converter instanceof JsonConverter ? SchemaType.JSON : SchemaType.AVRO)
                .properties(Collections.emptyMap())
                .schema(schema.toString().getBytes(UTF_8))
                .build();
        if (converter instanceof AvroConverter) {
            initializeAvroWriter(schema);
        }
    }

    @Override
    public byte[] encode(byte[] data) {
        if (null == valueConverter || valueConverter instanceof JsonConverter) {
            return data;
        }

        org.apache.kafka.connect.data.Schema connectSchema = avroData.toConnectSchema(avroSchema);
        JsonNode jsonNode = jsonDeserializer.deserialize("", data);

        Object connectValue;
        try {
            connectValue = convertToConnectMethod.invoke(
                    null,
                    connectSchema,
                    jsonNode
            );
        } catch (IllegalAccessException e) {
            throw new SchemaSerializationException("Can not call JsonConverter#convertToConnect");
        } catch (InvocationTargetException e) {
            throw new SchemaSerializationException(e.getCause());
        }

        Object avroValue = avroData.fromConnectData(
                connectSchema,
                connectValue
        );

        return writeAvroRecord((GenericRecord) avroValue);
    }

    private GenericDatumWriter<GenericRecord> writer;
    private BinaryEncoder encoder;
    private ByteArrayOutputStream byteArrayOutputStream;

    synchronized void initializeAvroWriter(org.apache.pulsar.kafka.shade.avro.Schema schema) {
        this.writer = new GenericDatumWriter<>(schema);
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(this.byteArrayOutputStream, this.encoder);
    }

    synchronized byte[] writeAvroRecord(GenericRecord record) {
        try {
            this.writer.write(record, this.encoder);
            this.encoder.flush();
            return this.byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new SchemaSerializationException(e);
        } finally {
            this.byteArrayOutputStream.reset();
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}
