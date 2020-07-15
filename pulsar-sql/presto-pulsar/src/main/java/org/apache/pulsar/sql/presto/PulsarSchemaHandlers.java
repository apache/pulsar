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


import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;

import io.prestosql.spi.PrestoException;
import java.util.List;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

class PulsarSchemaHandlers {

    static SchemaHandler newPulsarSchemaHandler(TopicName topicName,
                                                PulsarConnectorConfig pulsarConnectorConfig,
                                                SchemaInfo schemaInfo,
                                                List<PulsarColumnHandle> columnHandles) throws RuntimeException{
        if (schemaInfo.getType().isPrimitive()) {
            return new PulsarPrimitiveSchemaHandler(schemaInfo);
        } else if (schemaInfo.getType().isStruct()) {
            try {
                switch (schemaInfo.getType()) {
                    case JSON:
                        return new JSONSchemaHandler(columnHandles);
                    case AVRO:
                        return new AvroSchemaHandler(topicName, pulsarConnectorConfig, schemaInfo, columnHandles);
                    default:
                        throw new PrestoException(NOT_SUPPORTED, "Not supported schema type: " + schemaInfo.getType());
                }
            } catch (PulsarClientException e) {
                throw new RuntimeException(
                        new Throwable("PulsarAdmin gets version schema fail, topicName : "
                                + topicName.toString(), e));
            }
        } else if (schemaInfo.getType().equals(SchemaType.KEY_VALUE)) {
            return new KeyValueSchemaHandler(topicName, pulsarConnectorConfig, schemaInfo, columnHandles);
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
