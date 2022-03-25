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
package org.apache.pulsar.client.impl.schema.generic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GenericAvroReader implements SchemaReader<GenericRecord> {

    private final GenericDatumReader<GenericAvroRecord> reader;
    private BinaryEncoder encoder;
    private final ByteArrayOutputStream byteArrayOutputStream;
    private final List<Field> fields;
    private final Schema schema;
    private final byte[] schemaVersion;
    private final int offset;

    public GenericAvroReader(Schema schema) {
        this(null, schema, null);
    }

    public GenericAvroReader(Schema writerSchema, Schema readerSchema, byte[] schemaVersion) {
        this.schema = readerSchema;
        this.fields = schema.getFields()
                .stream()
                .map(f -> new Field(f.name(), f.pos()))
                .collect(Collectors.toList());
        this.schemaVersion = schemaVersion;
        if (writerSchema == null) {
            this.reader = new GenericDatumReader<>(readerSchema);
        } else {
            this.reader = new GenericDatumReader<>(writerSchema, readerSchema);
        }
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(this.byteArrayOutputStream, null);

        if (schema.getObjectProp(GenericAvroSchema.OFFSET_PROP) != null) {
            this.offset = Integer.parseInt(schema.getObjectProp(GenericAvroSchema.OFFSET_PROP).toString());
        } else {
            this.offset = 0;
        }

    }

    @Override
    public GenericAvroRecord read(byte[] bytes, int offset, int length) {
        try {
            if (offset == 0 && this.offset > 0) {
                offset = this.offset;
            }
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, offset, length - offset, null);
            org.apache.avro.generic.GenericRecord avroRecord =
                    (org.apache.avro.generic.GenericRecord) reader.read(
                    null,
                    decoder);
            return new GenericAvroRecord(schemaVersion, schema, fields, avroRecord);
        } catch (IOException | IndexOutOfBoundsException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public GenericRecord read(InputStream inputStream) {
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
            org.apache.avro.generic.GenericRecord avroRecord =
                    (org.apache.avro.generic.GenericRecord) reader.read(
                            null,
                            decoder);
            return new GenericAvroRecord(schemaVersion, schema, fields, avroRecord);
        } catch (IOException | IndexOutOfBoundsException e) {
            throw new SchemaSerializationException(e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                log.error("GenericAvroReader close inputStream close error", e);
            }
        }
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public Optional<Object> getNativeSchema() {
        return Optional.of(schema);
    }

    private static final Logger log = LoggerFactory.getLogger(GenericAvroReader.class);
}
