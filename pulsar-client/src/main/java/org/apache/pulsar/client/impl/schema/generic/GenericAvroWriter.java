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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaWriter;

public class GenericAvroWriter implements SchemaWriter<GenericRecord> {

    private final GenericDatumWriter<org.apache.avro.generic.GenericRecord> writer;
    private BinaryEncoder encoder;
    private final ByteArrayOutputStream byteArrayOutputStream;

    public GenericAvroWriter(Schema schema) {
        this.writer = new GenericDatumWriter<>(schema);
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(this.byteArrayOutputStream, null);
    }

    @Override
    public synchronized byte[] write(GenericRecord message) {
        try {
            writer.write(((GenericAvroRecord) message).getAvroRecord(), this.encoder);
            this.encoder.flush();
            return this.byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new SchemaSerializationException(e);
        } finally {
            this.byteArrayOutputStream.reset();
        }
    }
}
