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
package org.apache.pulsar.client.impl.schema.reader;

import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.pulsar.client.impl.schema.util.SchemaUtil.getJsr310ConversionEnabledFromSchemaInfo;
import static org.apache.pulsar.client.impl.schema.util.SchemaUtil.parseAvroSchema;

/**
 * A multi version avro reader.
 */
public class MultiVersionAvroReader<T> extends AbstractMultiVersionAvroBaseReader<T> {

    private final ClassLoader pojoClassLoader;

    public MultiVersionAvroReader(Schema readerSchema, ClassLoader pojoClassLoader, boolean jsr310ConversionEnabled) {
        super(new AvroReader<>(readerSchema, pojoClassLoader, jsr310ConversionEnabled), readerSchema);
        this.pojoClassLoader = pojoClassLoader;
    }

    @Override
    protected SchemaReader<T> loadReader(BytesSchemaVersion schemaVersion) {
        SchemaInfo schemaInfo = getSchemaInfoByVersion(schemaVersion.get());
        if (schemaInfo != null) {
            LOG.info("Load schema reader for version({}), schema is : {}, schemaInfo: {}",
                    SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                    schemaInfo.getSchemaDefinition(), schemaInfo.toString());
            boolean jsr310ConversionEnabled = getJsr310ConversionEnabledFromSchemaInfo(schemaInfo);
            return new AvroReader<>(parseAvroSchema(schemaInfo.getSchemaDefinition()),
                    readerSchema, pojoClassLoader, jsr310ConversionEnabled);
        } else {
            LOG.warn("No schema found for version({}), use latest schema : {}",
                    SchemaUtils.getStringSchemaVersion(schemaVersion.get()),
                    this.readerSchema);
            return providerSchemaReader;
        }
    }

    protected static final Logger LOG = LoggerFactory.getLogger(MultiVersionAvroReader.class);
}
