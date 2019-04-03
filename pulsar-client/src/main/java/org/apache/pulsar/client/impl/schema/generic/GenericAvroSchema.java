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


import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A generic avro schema.
 */
public class GenericAvroSchema extends GenericSchemaImpl {

    private DecodeType decodeType = DecodeType.COMP;

    public GenericAvroSchema(SchemaInfo schemaInfo) {
        super(schemaInfo,
                new GenericAvroWriter(schemaInfo),
                new GenericAvroReader(schemaInfo, new byte[10]),
                SchemaDefinition.builder()
                        .withSupportSchemaVersioning(true)
                        .build());
    }

    @Override
    public GenericRecordBuilder newRecordBuilder() {
        return new AvroRecordBuilderImpl(this);
    }

    @Override
    protected SchemaReader loadReader(byte[] schemaVersion) {
        return decodeType == DecodeType.AUTO ?
         new GenericAvroReader(new SchemaInfo().setSchema(((GenericAvroSchema) schemaProvider
                .getSchemaByVersion(schemaVersion)).getAvroSchema().toString().getBytes()),
                schemaVersion) :
         new GenericAvroReader(((GenericAvroSchema) schemaProvider
                .getSchemaByVersion(schemaVersion)).getAvroSchema(),
                schema,
                schemaVersion);
    }

    public GenericAvroSchema setConsumerType(DecodeType decodeType) {
        this.decodeType = decodeType;
        return this;
    }
}
