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
package org.apache.pulsar.sql.presto;

import io.netty.buffer.ByteBuf;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.pulsar.client.impl.schema.AbstractSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A presto schema handler that interprets data using pulsar schema.
 */
public class PulsarPrimitiveSchemaHandler implements SchemaHandler {

    private final SchemaInfo schemaInfo;
    private final AbstractSchema<?> schema;

    PulsarPrimitiveSchemaHandler(SchemaInfo schemaInfo) {
        this.schemaInfo = schemaInfo;
        this.schema = (AbstractSchema<?>) AutoConsumeSchema.getSchema(schemaInfo);
    }

    @Override
    public Object deserialize(ByteBuf payload) {
        Object currentRecord = schema.decode(payload);
        switch (schemaInfo.getType()) {
            case DATE:
                return ((Date) currentRecord).getTime();
            case TIME:
                return ((Time) currentRecord).getTime();
            case TIMESTAMP:
                return ((Timestamp) currentRecord).getTime();
            default:
                return currentRecord;
        }
    }

    @Override
    public Object extractField(int index, Object currentRecord) {
        return currentRecord;
    }
}