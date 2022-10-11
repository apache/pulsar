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
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.protocol.schema.SchemaHash;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Information about the schema.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Data
@NoArgsConstructor
@Accessors(chain = true)
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
     * The created time of schema.
     */
    private long timestamp;

    /**
     * Additional properties of the schema definition (implementation defined).
     */
    private Map<String, String> properties = Collections.emptyMap();

    @EqualsAndHashCode.Exclude
    @JsonIgnore
    private transient SchemaHash schemaHash;

    @Builder
    public SchemaInfoImpl(String name, byte[] schema, SchemaType type, long timestamp,
                          Map<String, String> properties) {
        this.name = name;
        this.schema = schema;
        this.type = type;
        this.timestamp = timestamp;
        this.properties = properties == null ? Collections.emptyMap() : properties;
        this.schemaHash = SchemaHash.of(this.schema, this.type);
    }

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

    /**
     * Calculate the SchemaHash for compatible with `@NoArgsConstructor`.
     * If SchemaInfoImpl is created by no-args-constructor from users, the schemaHash will be null.
     * Note: We should remove this method as long as `@NoArgsConstructor` removed at major release to avoid null-check
     * overhead.
     */
    public SchemaHash getSchemaHash() {
        if (schemaHash == null) {
            schemaHash = SchemaHash.of(this.schema, this.type);
        }
        return schemaHash;
    }

    @Override
    public String toString() {
        return SchemaUtils.jsonifySchemaInfo(this);
    }
}
