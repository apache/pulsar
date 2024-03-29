/*
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

import static org.apache.pulsar.client.impl.schema.generic.MultiVersionGenericProtobufNativeReader.parseProtobufSchema;
import com.google.protobuf.Descriptors;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Generic ProtobufNative schema.
 */
@Slf4j
public class GenericProtobufNativeSchema extends AbstractGenericSchema {

    Descriptors.Descriptor descriptor;

    public GenericProtobufNativeSchema(SchemaInfo schemaInfo) {
        this(schemaInfo, true);
    }

    public GenericProtobufNativeSchema(SchemaInfo schemaInfo,
                                       boolean useProvidedSchemaAsReaderSchema) {
        super(schemaInfo, useProvidedSchemaAsReaderSchema);
        this.descriptor = parseProtobufSchema(schemaInfo);
        this.fields = descriptor.getFields()
                .stream()
                .map(f -> new Field(f.getName(), f.getIndex()))
                .collect(Collectors.toList());
        setReader(new MultiVersionGenericProtobufNativeReader(useProvidedSchemaAsReaderSchema, schemaInfo));
        setWriter(new GenericProtobufNativeWriter());
    }

    @Override
    public GenericRecordBuilder newRecordBuilder() {
        return new ProtobufNativeRecordBuilderImpl(this);
    }

    public static GenericSchema of(SchemaInfo schemaInfo) {
        return new GenericProtobufNativeSchema(schemaInfo);
    }

    public static GenericSchema of(SchemaInfo schemaInfo, boolean useProvidedSchemaAsReaderSchema) {
        return new GenericProtobufNativeSchema(schemaInfo, useProvidedSchemaAsReaderSchema);
    }

    public Descriptors.Descriptor getProtobufNativeSchema() {
        return descriptor;
    }

    @Override
    public Optional<Object> getNativeSchema() {
        return Optional.of(descriptor);
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

}
