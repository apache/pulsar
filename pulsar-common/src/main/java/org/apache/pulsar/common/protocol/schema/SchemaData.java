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
package org.apache.pulsar.common.protocol.schema;

import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Schema data.
 */
@Builder
@Data
public class SchemaData {
    private final SchemaType type;
    private final boolean isDeleted;
    private final long timestamp;
    private final String user;
    private final byte[] data;
    @Builder.Default
    private Map<String, String> props = new HashMap<>();

    /**
     * Convert a schema data to a schema info.
     *
     * @return the converted schema info.
     */
    public SchemaInfo toSchemaInfo() {
        return SchemaInfo.builder()
            .name("")
            .type(type)
            .schema(data)
            .properties(props)
            .build();
    }

    /**
     * Convert a schema info to a schema data.
     *
     * @param schemaInfo schema info
     * @return the converted schema schema data
     */
    public static SchemaData fromSchemaInfo(SchemaInfo schemaInfo) {
        return SchemaData.builder()
            .type(schemaInfo.getType())
            .data(schemaInfo.getSchema())
            .props(schemaInfo.getProperties())
            .build();
    }

}