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
package org.apache.pulsar.client.impl.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Information about the schema.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
@Builder
public class SchemaInfoImpl implements SchemaInfo {

    @EqualsAndHashCode.Exclude
    private String name;

    /**
     * The schema data in AVRO JSON format.
     */
    private byte[] schema;

    /**
     * The type of schema (AVRO, JSON, PROTOBUF, etc..).
     */
    private SchemaType type;

    /**
     * Additional properties of the schema definition (implementation defined).
     */
    @Builder.Default
    private Map<String, String> properties = Collections.emptyMap();

    public String getSchemaDefinition() {
        if (null == schema) {
            return "";
        }

        switch (type) {
            case AVRO:
            case JSON:
            case PROTOBUF:
            case PROTOBUF_NATIVE:
                return new String(schema, UTF_8);
            case KEY_VALUE:
                KeyValue<SchemaInfo, SchemaInfo> schemaInfoKeyValue = KeyValueSchemaInfo.decodeKeyValueSchemaInfo(this);
                return SchemaUtils.jsonifyKeyValueSchemaInfo(schemaInfoKeyValue);
            default:
                return Base64.getEncoder().encodeToString(schema);
        }
    }

    @Override
    public String toString() {
        return SchemaUtils.jsonifySchemaInfo(this);
    }
}
