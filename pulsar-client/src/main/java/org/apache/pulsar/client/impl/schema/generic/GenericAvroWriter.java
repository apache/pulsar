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

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaWriter;

import java.io.ByteArrayOutputStream;
@Slf4j
public class GenericAvroWriter implements SchemaWriter<GenericRecord> {

    private final GenericDatumWriter<org.apache.avro.generic.GenericRecord> writer;
    private final BinaryEncoder encoder;
    private final ByteArrayOutputStream byteArrayOutputStream;
    private final GenericRecordAdapter adapter;

    public GenericAvroWriter(Schema schema) {
        this.adapter = new GenericRecordAdapter(schema);
        this.writer = new GenericDatumWriter<>(schema);
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(this.byteArrayOutputStream, null);
    }

    @Override
    public synchronized byte[] write(GenericRecord message) {
        try {
            if (message instanceof GenericAvroRecord) {
                writer.write(((GenericAvroRecord) message).getAvroRecord(), this.encoder);
            } else {
                adapter.setCurrentMessage(message);
                try {
                    writer.write(adapter, this.encoder);
                } finally {
                    adapter.setCurrentMessage(null);
                }
            }
            this.encoder.flush();
            return this.byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            log.error("error", e);
            throw new SchemaSerializationException(e);
        } finally {
            this.byteArrayOutputStream.reset();
        }
    }

    /**
     * This is an adapter from Pulsar GenericRecord to Avro classes.
     */
    private class GenericRecordAdapter extends SpecificRecordBase {
        private GenericRecord message;
        private final Schema schema;

        public GenericRecordAdapter(Schema schema) {
            this.schema = schema;
        }

        void setCurrentMessage(GenericRecord message) {
            this.message = message;
        }
        @Override
        public Schema getSchema() {
            return schema;
        }

        @Override
        public Object get(int field) {
            return message.getField(schema.getFields().get(field).name());
        }

        @Override
        public void put(int field, Object value) {
            throw new UnsupportedOperationException();
        }
    }
}
