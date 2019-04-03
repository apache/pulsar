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

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.io.ByteArrayOutputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

public class GenericAvroWriter<T> implements SchemaWriter<T> {

    private final GenericDatumWriter writer;
    private BinaryEncoder encoder;
    private final ByteArrayOutputStream byteArrayOutputStream;

    public GenericAvroWriter(SchemaInfo schemaInfo) {
        this.writer = new GenericDatumWriter<>(new org.apache.avro.Schema.Parser().parse(
                new String(schemaInfo.getSchema(), UTF_8)));
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(this.byteArrayOutputStream, encoder);
    }

    @Override
    public synchronized byte[] write(T message) {
        checkArgument(message instanceof GenericAvroRecord);
        GenericAvroRecord gar = (GenericAvroRecord) message;
        try {
            writer.write(gar.getAvroRecord(), this.encoder);
            this.encoder.flush();
            return this.byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            throw new SchemaSerializationException(e);
        } finally {
            this.byteArrayOutputStream.reset();
        }
    }
}
