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
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A generic json schema.
 */
@Slf4j
public class GenericJsonSchema extends GenericSchemaImpl {

    public GenericJsonSchema(SchemaInfo schemaInfo) {
        this(schemaInfo, true);
    }

    GenericJsonSchema(SchemaInfo schemaInfo,
                      boolean useProvidedSchemaAsReaderSchema) {
        super(schemaInfo);
        setWriter(new GenericJsonWriter());
        setReader(new MultiVersionGenericJsonReader(useProvidedSchemaAsReaderSchema, schema, schemaInfo, fields));
    }

    @Override
    public GenericRecordBuilder newRecordBuilder() {
        return new JsonRecordBuilderImpl(this);
    }

    public boolean supportSchemaVersioning() {
        return true;
    }

    public Schema<GenericRecord> clone() {
        Schema<GenericRecord> schema = of(this.schemaInfo,
                ((AbstractMultiVersionGenericReader) this.reader).useProvidedSchemaAsReaderSchema);
        if (this.schemaInfoProvider != null) {
            schema.setSchemaInfoProvider(this.schemaInfoProvider);
        }
        return schema;
    }
}
