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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.client.impl.schema.reader.AbstractMultiVersionReader;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;


@Slf4j
public abstract class AbstractMultiVersionGenericProtobufReader extends AbstractMultiVersionReader<GenericRecord>
        implements SchemaReader<GenericRecord>{
    private final boolean useProvidedSchemaAsReaderSchema;
    private final SchemaInfo schemaInfo;
    private final Descriptors.Descriptor descriptor;

    public AbstractMultiVersionGenericProtobufReader(boolean useProvidedSchemaAsReaderSchema,
                                                     SchemaInfo schemaInfo,
                                                     SchemaReader<GenericRecord> protobufReader) {
        super(protobufReader);
        this.useProvidedSchemaAsReaderSchema = useProvidedSchemaAsReaderSchema;
        this.schemaInfo = schemaInfo;
        this.descriptor = (Descriptors.Descriptor) providerSchemaReader.getNativeSchema()
                .orElseThrow(() -> {
                    log.error(" No protobuf reader found!");
                    return new IllegalArgumentException("No protobuf reader found!");
                });
    }

    @Override
    protected SchemaReader<GenericRecord> loadReader(BytesSchemaVersion schemaVersion) {
        SchemaInfo schemaInfo = getSchemaInfoByVersion(schemaVersion.get());
        if (schemaInfo != null) {
            log.info("Load schema reader for version({}), schema is : {}",
                    SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                    schemaInfo);
            Descriptors.Descriptor recordDescriptor = parserSchemaInfoToDescriptor(schemaInfo);
            Descriptors.Descriptor readerSchemaDescriptor =
                    useProvidedSchemaAsReaderSchema ? descriptor : recordDescriptor;
            return instanceReader(readerSchemaDescriptor, schemaVersion.get());
        } else {
            log.warn("No schema found for version({}), use latest schema : {}",
                    SchemaUtils.getStringSchemaVersion(schemaVersion.get()), this.schemaInfo);
            return providerSchemaReader;
        }
    }

    protected abstract Descriptors.Descriptor parserSchemaInfoToDescriptor(SchemaInfo schemaInfo);

    protected abstract AbstractGenericProtobufReader instanceReader(
            Descriptors.Descriptor descriptor, byte[] schemaVersion);
}
