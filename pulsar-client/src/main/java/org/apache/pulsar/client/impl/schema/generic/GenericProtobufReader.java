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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Generic protobuf reader.
 *
 * @see GenericProtobufNativeReader
 */
@Slf4j
public class GenericProtobufReader implements SchemaReader<GenericRecord> {
    private final Descriptors.Descriptor descriptor;
    private final byte[] schemaVersion;
    private final List<Field> fields;

    /**
     * Create a new Generic protobuf reader by protobuf descriptor.
     *
     * @param descriptor protobuf descriptor
     * @see Descriptors##descriptor
     */
    public GenericProtobufReader(Descriptors.Descriptor descriptor) {
        this(descriptor, null);
    }

    /**
     * Create a new Generic protobuf reader by protobuf descriptor and schema version.
     *
     * @param descriptor protobuf descriptor
     * @see Descriptors##descriptor
     */
    public GenericProtobufReader(Descriptors.Descriptor descriptor, byte[] schemaVersion) {
        try {
            this.schemaVersion = schemaVersion;
            this.descriptor = descriptor;
            this.fields = descriptor.getFields()
                    .stream()
                    .map(f -> new Field(f.getName(), f.getIndex()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("GenericProtobufReader init error", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode data by data bytes, offset and length
     *
     * @param bytes  the data
     * @param offset the byte[] initial position
     * @param length the byte[] read length
     * @return GenericProtobufRecord
     * @see GenericProtobufRecord
     */
    @Override
    public GenericProtobufRecord read(byte[] bytes, int offset, int length) {
        try {
            if (!(bytes.length == length && offset == 0)) {
                bytes = Arrays.copyOfRange(bytes, offset, offset + length);
            }
            return new GenericProtobufRecord(schemaVersion, descriptor,
                    fields, DynamicMessage.parseFrom(descriptor, bytes));
        } catch (InvalidProtocolBufferException e) {
            throw new SchemaSerializationException(e);
        }
    }

    /**
     * Decode data by data bytes, offset and length
     *
     * @param inputStream the stream of message
     * @return GenericProtobufRecord
     * @see GenericProtobufRecord
     */
    @Override
    public GenericProtobufRecord read(InputStream inputStream) {
        try {
            return new GenericProtobufRecord(schemaVersion, descriptor,
                    fields, DynamicMessage.parseFrom(descriptor, inputStream));
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        }
    }

    /**
     * Get native protobuf schema.
     *
     * @return native schema
     */
    @Override
    public Optional<Object> getNativeSchema() {
        return Optional.of(descriptor);
    }

}
