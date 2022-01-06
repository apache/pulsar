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
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.client.impl.schema.reader.AbstractMultiVersionReader;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A multi version generic protobuf-native reader.
 */
@Slf4j
public class MultiVersionGenericProtobufNativeReader
    extends AbstractMultiVersionReader<GenericRecord>
    implements SchemaReader<GenericRecord> {

    // the flag controls whether to use the provided schema as reader schema
    // to decode the messages. In `AUTO_CONSUME` mode, setting this flag to `false`
    // allows decoding the messages using the schema associated with the messages.
    private final boolean useProvidedSchemaAsReaderSchema;
    private final SchemaInfo schemaInfo;
    private final Descriptors.Descriptor descriptor;

    public MultiVersionGenericProtobufNativeReader(boolean useProvidedSchemaAsReaderSchema,
                                                   SchemaInfo schemaInfo) {
        super(new GenericProtobufNativeReader(parseProtobufSchema(schemaInfo)));
        this.useProvidedSchemaAsReaderSchema = useProvidedSchemaAsReaderSchema;
        this.schemaInfo = schemaInfo;
        this.descriptor = (Descriptors.Descriptor) providerSchemaReader.getNativeSchema()
                .orElseThrow(()->{
                    log.error("No protobuf native reader found.");
                    return new IllegalArgumentException("No protobuf native reader found.");
                });
    }

    @Override
    protected SchemaReader<GenericRecord> loadReader(BytesSchemaVersion schemaVersion) {
        SchemaInfo schemaInfo = getSchemaInfoByVersion(schemaVersion.get());
        if (schemaInfo != null) {
            log.info("Load schema reader for version({}), schema is : {}",
                    SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                    schemaInfo);
            Descriptors.Descriptor recordDescriptor = parseProtobufSchema(schemaInfo);
            Descriptors.Descriptor readerSchemaDescriptor =
                useProvidedSchemaAsReaderSchema ? descriptor : recordDescriptor;
            return new GenericProtobufNativeReader(
                    readerSchemaDescriptor,
                    schemaVersion.get());
        } else {
            log.warn("No schema found for version({}), use latest schema : {}",
                    SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                    this.schemaInfo);
            return providerSchemaReader;
        }
    }

    protected static Descriptors.Descriptor parseProtobufSchema(SchemaInfo schemaInfo) {
        return ProtobufNativeSchemaUtils.deserialize(schemaInfo.getSchema());
    }
}
