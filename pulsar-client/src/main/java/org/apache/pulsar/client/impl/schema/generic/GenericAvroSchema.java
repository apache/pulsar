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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A generic avro schema.
 */
@Slf4j
public class GenericAvroSchema extends GenericSchemaImpl {

    public static final String OFFSET_PROP = "__AVRO_READ_OFFSET__";

    public GenericAvroSchema(SchemaInfo schemaInfo) {
        this(schemaInfo, true);
    }

    GenericAvroSchema(SchemaInfo schemaInfo,
                      boolean useProvidedSchemaAsReaderSchema) {
        super(schemaInfo);
        setReader(new MultiVersionGenericAvroReader(useProvidedSchemaAsReaderSchema, schema));
        setWriter(new GenericAvroWriter(schema));

        if (schemaInfo.getProperties().containsKey(GenericAvroSchema.OFFSET_PROP)) {
            this.schema.addProp(GenericAvroSchema.OFFSET_PROP,
                    schemaInfo.getProperties().get(GenericAvroSchema.OFFSET_PROP));
        }
    }

    @Override
    public GenericRecordBuilder newRecordBuilder() {
        return new AvroRecordBuilderImpl(this);
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

    @Override
    public org.apache.pulsar.client.api.Schema<GenericRecord> clone() {
        org.apache.pulsar.client.api.Schema<GenericRecord> schema =
                GenericAvroSchema.of(schemaInfo,
                        ((AbstractMultiVersionGenericReader) reader).useProvidedSchemaAsReaderSchema);
        if (schemaInfoProvider != null) {
            schema.setSchemaInfoProvider(schemaInfoProvider);
        }
        return schema;
    }
}
