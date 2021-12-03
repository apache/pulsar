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
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import java.util.stream.Collectors;
import static org.apache.pulsar.client.impl.schema.generic
        .MultiVersionGenericProtobufReader.parseAvroBaseSchemaToProtobuf;

/**
 * Generic protobuf schema.
 */
public class GenericProtobufSchema extends AbstractGenericSchema {
    Descriptors.Descriptor descriptor;

    /**
     * Create a new generic protobuf schema by schema info.
     *
     * @param schemaInfo schema information
     * @see SchemaInfo
     */
    public GenericProtobufSchema(SchemaInfo schemaInfo) {
        this(schemaInfo, true);
    }

    /**
     * Create a new generic protobuf schema by schema info and whether useProvidedSchemaAsReaderSchema.
     *
     * @param schemaInfo                      schema information
     * @param useProvidedSchemaAsReaderSchema whether we use provided schema as reader schema
     * @see SchemaInfo
     */
    public GenericProtobufSchema(SchemaInfo schemaInfo,
                                 boolean useProvidedSchemaAsReaderSchema) {
        super(schemaInfo, useProvidedSchemaAsReaderSchema);
        this.descriptor = parseAvroBaseSchemaToProtobuf(schemaInfo);
        this.fields = descriptor.getFields()
                .stream()
                .map(f -> new Field(f.getName(), f.getIndex()))
                .collect(Collectors.toList());
        setReader(new MultiVersionGenericProtobufReader(useProvidedSchemaAsReaderSchema, schemaInfo));
        setWriter(new GenericProtobufWriter());
    }

    @Override
    public GenericRecordBuilder newRecordBuilder() {
        return new ProtobufRecordBuilderImpl(this);
    }

    public static GenericSchema<?> of(SchemaInfo schemaInfo) {
        return new GenericProtobufSchema(schemaInfo);
    }

    public static GenericSchema<?> of(SchemaInfo schemaInfo, boolean useProvidedSchemaAsReaderSchema) {
        return new GenericProtobufSchema(schemaInfo, useProvidedSchemaAsReaderSchema);
    }

    public Descriptors.Descriptor getProtobufSchema() {
        return descriptor;
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }
}
