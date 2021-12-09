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
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.common.schema.SchemaInfo;
import java.util.stream.Collectors;

public abstract class AbstractGenericProtobufSchema extends AbstractGenericSchema {
    Descriptors.Descriptor descriptor;

    public AbstractGenericProtobufSchema(SchemaInfo schemaInfo,Descriptors.Descriptor descriptor) {
        this(schemaInfo, true,descriptor);
    }

    public AbstractGenericProtobufSchema(SchemaInfo schemaInfo,
                                         boolean useProvidedSchemaAsReaderSchema,
                                         Descriptors.Descriptor descriptor) {
        super(schemaInfo, useProvidedSchemaAsReaderSchema);
        this.descriptor = descriptor;
        this.fields = descriptor.getFields()
                .stream()
                .map(f -> new Field(f.getName(), f.getIndex()))
                .collect(Collectors.toList());
        setReader(instanceReader());
        setWriter(instanceWriter());
    }

    public Descriptors.Descriptor getProtobufBaseSchema() {
        return descriptor;
    }

    protected abstract SchemaReader<GenericRecord> instanceReader();

    protected abstract SchemaWriter<GenericRecord> instanceWriter();

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }
}
