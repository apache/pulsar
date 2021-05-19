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

import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pulsar.client.impl.schema.util.SchemaUtil.parseAvroSchema;

/**
 * A multi version generic avro reader.
 */
public class MultiVersionGenericAvroReader extends AbstractMultiVersionGenericReader {

    public MultiVersionGenericAvroReader(boolean useProvidedSchemaAsReaderSchema, Schema readerSchema) {
        super(useProvidedSchemaAsReaderSchema, new GenericAvroReader(readerSchema), readerSchema);
    }

    @Override
    protected SchemaReader<GenericRecord> loadReader(BytesSchemaVersion schemaVersion) {
        SchemaInfo schemaInfo = getSchemaInfoByVersion(schemaVersion.get());
        if (schemaInfo != null) {
            LOG.info("Load schema reader for version({}), schema is : {}",
                    SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                    schemaInfo);
            Schema writerSchema = parseAvroSchema(schemaInfo.getSchemaDefinition());
            Schema readerSchema = useProvidedSchemaAsReaderSchema ? this.readerSchema : writerSchema;
            readerSchema.addProp(GenericAvroSchema.OFFSET_PROP,
                    schemaInfo.getProperties().getOrDefault(GenericAvroSchema.OFFSET_PROP, "0"));

            return new GenericAvroReader(
                    writerSchema,
                    readerSchema,
                    schemaVersion.get());
        } else {
            LOG.warn("No schema found for version({}), use latest schema : {}",
                    SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                    this.readerSchema);
            return providerSchemaReader;
        }
    }
    protected static final Logger LOG = LoggerFactory.getLogger(MultiVersionGenericAvroReader.class);
}
