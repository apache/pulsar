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

import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.client.impl.schema.ProtobufSchemaUtils;
import org.apache.pulsar.common.schema.SchemaInfo;

public class GenericProtobufSchema extends AbstractGenericProtobufSchema {

    public GenericProtobufSchema(SchemaInfo schemaInfo) {
        super(schemaInfo, ProtobufSchemaUtils.parseAvroBaseSchemaInfoToProtobufDescriptor(schemaInfo.getSchema()));
    }

    public GenericProtobufSchema(SchemaInfo schemaInfo, boolean useProvidedSchemaAsReaderSchema) {
        super(schemaInfo, ProtobufSchemaUtils.parseAvroBaseSchemaInfoToProtobufDescriptor(schemaInfo.getSchema()));
    }

    @Override
    protected SchemaReader<GenericRecord> instanceReader() {
        return new MultiVersionGenericProtobufReader(useProvidedSchemaAsReaderSchema, schemaInfo);
    }

    @Override
    protected SchemaWriter<GenericRecord> instanceWriter() {
        return new GenericProtobufBaseWriter();
    }

    @Override
    public GenericRecordBuilder newRecordBuilder() {
        return new ProtobufRecordBuilderImpl(this);
    }

    public static GenericSchema<GenericRecord> of(SchemaInfo schemaInfo) {
        return new GenericProtobufSchema(schemaInfo);
    }

    public static GenericSchema<GenericRecord> of(SchemaInfo schemaInfo, boolean useProvidedSchemaAsReaderSchema) {
        return new GenericProtobufSchema(schemaInfo, useProvidedSchemaAsReaderSchema);
    }
}
