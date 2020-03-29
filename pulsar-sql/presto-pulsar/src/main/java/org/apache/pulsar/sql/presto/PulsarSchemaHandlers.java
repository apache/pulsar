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

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.facebook.presto.spi.PrestoException;
import java.util.List;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

class PulsarSchemaHandlers {

    static SchemaHandler newPulsarSchemaHandler(SchemaInfo schemaInfo,
                                                List<PulsarColumnHandle> columnHandles) {
        if (schemaInfo.getType().isPrimitive()) {
            return new PulsarPrimitiveSchemaHandler(schemaInfo);
        } else if (schemaInfo.getType().isStruct()) {
            switch (schemaInfo.getType()) {
                case JSON:
                    return new JSONSchemaHandler(columnHandles);
                case AVRO:
                    return new AvroSchemaHandler(PulsarConnectorUtils
                            .parseSchema(new String(schemaInfo.getSchema(), UTF_8)
                    ), columnHandles);
                default:
                    throw new PrestoException(NOT_SUPPORTED, "Not supported schema type: " + schemaInfo.getType());
            }

        } else if (schemaInfo.getType().equals(SchemaType.KEY_VALUE)) {
            return new KeyValueSchemaHandler(schemaInfo, columnHandles);
        } else {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    "Schema `" + schemaInfo.getType() + "` is not supported by presto yet : " + schemaInfo);
        }
    }

    static SchemaInfo defaultSchema() {
        return Schema.BYTES.getSchemaInfo();
    }
}