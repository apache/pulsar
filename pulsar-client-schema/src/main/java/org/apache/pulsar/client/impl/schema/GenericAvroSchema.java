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
package org.apache.pulsar.client.impl.schema;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A generic avro schema.
 */
public class GenericAvroSchema implements Schema<GenericRecord> {

    private final org.apache.avro.Schema schema;
    private final List<Field> fields;
    private final SchemaInfo schemaInfo;
    private final GenericDatumWriter<org.apache.avro.generic.GenericRecord> datumWriter;
    private BinaryEncoder encoder;
    private final ByteArrayOutputStream byteArrayOutputStream;
    private final GenericDatumReader<org.apache.avro.generic.GenericRecord> datumReader;

    public GenericAvroSchema(SchemaInfo schemaInfo) {
        this.schemaInfo = schemaInfo;
        this.schema = new org.apache.avro.Schema.Parser().parse(
            new String(schemaInfo.getSchema(), UTF_8)
        );
        this.fields = schema.getFields()
            .stream()
            .map(f -> new Field(f.name(), f.pos()))
            .collect(Collectors.toList());
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(this.byteArrayOutputStream, encoder);
        this.datumWriter = new GenericDatumWriter(schema);
        this.datumReader = new GenericDatumReader(schema);
    }

    public org.apache.avro.Schema getAvroSchema() {
        return schema;
    }

    @Override
    public synchronized byte[] encode(GenericRecord message) {
        checkArgument(message instanceof GenericAvroRecord);
        GenericAvroRecord gar = (GenericAvroRecord) message;
        try {
            datumWriter.write(gar.getAvroRecord(), this.encoder);
            this.encoder.flush();
            return this.byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new SchemaSerializationException(e);
        } finally {
            this.byteArrayOutputStream.reset();
        }
    }

    @Override
    public GenericRecord decode(byte[] bytes) {
        try {
            org.apache.avro.generic.GenericRecord avroRecord = datumReader.read(
                null,
                DecoderFactory.get().binaryDecoder(bytes, null));
            return new GenericAvroRecord(schema, fields, avroRecord);
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}
