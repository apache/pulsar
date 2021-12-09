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
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


public abstract class AbstractGenericProtobufReader implements SchemaReader<GenericRecord> {
    private final Descriptors.Descriptor descriptor;
    private final byte[] schemaVersion;
    private final List<Field> fields;


    public AbstractGenericProtobufReader(Descriptors.Descriptor descriptor) {
        this(descriptor, null);
    }


    public AbstractGenericProtobufReader(Descriptors.Descriptor descriptor, byte[] schemaVersion) {
        this.schemaVersion = schemaVersion;
        this.descriptor = descriptor;
        this.fields = descriptor.getFields()
                .stream()
                .map(f -> new Field(f.getName(), f.getIndex()))
                .collect(Collectors.toList());
    }


    @Override
    public AbstractGenericProtobufRecord read(byte[] bytes, int offset, int length) {
        try {
            if (!(bytes.length == length && offset == 0)) { //skip unnecessary bytes copy
                bytes = Arrays.copyOfRange(bytes, offset, offset + length);
            }
            DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, bytes);
            return instanceRecord(schemaVersion, descriptor, fields, dynamicMessage);
        } catch (InvalidProtocolBufferException e) {
            throw new SchemaSerializationException(e);
        }
    }


    @Override
    public AbstractGenericProtobufRecord read(InputStream inputStream) {
        try {
            DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, inputStream);
            return instanceRecord(schemaVersion, descriptor, fields, dynamicMessage);
        } catch (IOException e) {
            throw new SchemaSerializationException(e);
        }
    }

    protected abstract AbstractGenericProtobufRecord instanceRecord(byte[] schemaVersion, Descriptors.Descriptor msgDesc,
                                                                    List<Field> fields, DynamicMessage record);

    @Override
    public Optional<Object> getNativeSchema() {
        return Optional.of(descriptor);
    }
}
