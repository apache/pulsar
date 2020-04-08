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
package org.apache.pulsar.client.impl.schema.writer;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaWriter;

import java.io.ByteArrayOutputStream;

public class AvroWriter<T> implements SchemaWriter<T> {
    private final ReflectDatumWriter<T> writer;
    private BinaryEncoder encoder;
    private ByteArrayOutputStream byteArrayOutputStream;

    public AvroWriter(Schema schema) {
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(this.byteArrayOutputStream, this.encoder);
        this.writer = new ReflectDatumWriter<>(schema);
    }

    @Override
    public synchronized byte[] write(T message) {
        try {
            writer.write(message, this.encoder);
            this.encoder.flush();
            return this.byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new SchemaSerializationException(e);
        } finally {
            try {
                this.encoder.flush();
            } catch (IOException e) {
                throw new SchemaSerializationException(e);
            }
            this.byteArrayOutputStream.reset();
        }
    }
}
