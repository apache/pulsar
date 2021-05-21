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

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class GenericProtobufNativeReader implements SchemaReader<GenericRecord> {

    private final Descriptors.Descriptor descriptor;
    private final byte[] schemaVersion;
    private final List<Field> fields;

    public GenericProtobufNativeReader(Descriptors.Descriptor descriptor) {
        this(descriptor, null);
    }

    public GenericProtobufNativeReader(Descriptors.Descriptor descriptor, byte[] schemaVersion) {
        try {
            this.schemaVersion = schemaVersion;
            this.descriptor = descriptor;
            this.fields = descriptor.getFields()
                    .stream()
                    .map(f -> new Field(f.getName(), f.getIndex()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("GenericProtobufNativeReader init error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public GenericProtobufNativeRecord read(byte[] bytes, int offset, int length) {
        try {
            if (!(bytes.length == length && offset == 0)) { //skip unnecessary bytes copy
                bytes = Arrays.copyOfRange(bytes, offset, offset + length);
            }
            return new GenericProtobufNativeRecord(schemaVersion, descriptor, fields, DynamicMessage.parseFrom(descriptor, bytes));
        } catch (InvalidProtocolBufferException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public GenericProtobufNativeRecord read(InputStream inputStream) {
        try {
            return new GenericProtobufNativeRecord(schemaVersion, descriptor, fields, DynamicMessage.parseFrom(descriptor, inputStream));
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public Optional<Object> getNativeSchema() {
        return Optional.of(descriptor);
    }

    private static final Logger log = LoggerFactory.getLogger(GenericProtobufNativeReader.class);
}
