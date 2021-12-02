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
import org.apache.avro.Schema;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.client.impl.schema.reader.AbstractMultiVersionReader;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;

@Slf4j
public class MultiVersionGenericProtobufReader extends AbstractMultiVersionReader<GenericRecord>
        implements SchemaReader<GenericRecord> {

    private final boolean useProvidedSchemaAsReaderSchema;
    private final SchemaInfo schemaInfo;
    private final Descriptors.Descriptor descriptor;


    public MultiVersionGenericProtobufReader(boolean useProvidedSchemaAsReaderSchema,
                                             SchemaInfo schemaInfo) {
        super(new GenericProtobufReader(parseAvroBaseSchemaToProtobuf(schemaInfo)));
        this.useProvidedSchemaAsReaderSchema = useProvidedSchemaAsReaderSchema;
        this.schemaInfo = schemaInfo;
        this.descriptor = parseAvroBaseSchemaToProtobuf(schemaInfo);
    }

    @Override
    protected SchemaReader<GenericRecord> loadReader(BytesSchemaVersion schemaVersion) {
        SchemaInfo schemaInfo = getSchemaInfoByVersion(schemaVersion.get());
        if (schemaInfo != null) {
            log.info("Load schema reader for version({}), schema is : {}",
                    SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                    schemaInfo);
            Descriptors.Descriptor recordDescriptor = parseAvroBaseSchemaToProtobuf(schemaInfo);
            Descriptors.Descriptor readerSchemaDescriptor =
                    useProvidedSchemaAsReaderSchema ? descriptor : recordDescriptor;
            return new GenericProtobufReader(readerSchemaDescriptor, schemaVersion.get());
        } else {
            log.warn("No schema found for version({}), use latest schema : {}",
                    SchemaUtils.getStringSchemaVersion(schemaVersion.get()), this.schemaInfo);
            return providerSchemaReader;
        }
    }

    /**
     * Parse protobuf schema by avro serialized schema information.
     *
     * @param schemaInfo schema information
     * @return protobuf descriptor
     */
    protected static Descriptors.Descriptor parseAvroBaseSchemaToProtobuf(SchemaInfo schemaInfo) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(new String(schemaInfo.getSchema(), StandardCharsets.UTF_8));
        Object pojo = ProtobufData.get().newRecord(null, schema);
        try {
            Method getDescriptorForType = pojo.getClass().getMethod("getDescriptorForType");
            return (Descriptors.Descriptor) getDescriptorForType.invoke(pojo);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.error("parse protobuf schema by schema information fail, the schema info is {}",
                    schemaInfo);
            throw new SchemaSerializationException(e);
        }
    }
}
