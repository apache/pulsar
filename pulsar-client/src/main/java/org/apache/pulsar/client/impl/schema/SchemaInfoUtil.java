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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import lombok.experimental.UtilityClass;

import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.Schema;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.schema.GetSchemaResponse;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Class helping to initialize schemas.
 */
@UtilityClass
public class SchemaInfoUtil {

    public static SchemaInfo newSchemaInfo(String name, SchemaData data) {
        return SchemaInfoImpl.builder()
                .name(name)
                .schema(data.getData())
                .type(data.getType())
                .properties(data.getProps())
                .build();
    }

    public static SchemaInfo newSchemaInfo(Schema schema) {
        SchemaInfoImpl.SchemaInfoImplBuilder si = SchemaInfoImpl.builder()
                .name(schema.getName())
                .schema(schema.getSchemaData())
                .type(Commands.getSchemaType(schema.getType()));
        if (schema.getPropertiesCount() == 0) {
            si.properties(Collections.emptyMap());
        } else {
            Map<String, String> properties = new TreeMap<>();
            for (int i = 0; i < schema.getPropertiesCount(); i++) {
                KeyValue kv = schema.getPropertyAt(i);
                properties.put(kv.getKey(), kv.getValue());
            }

            si.properties(properties);
        }
        return si.build();
    }

    public static SchemaInfo newSchemaInfo(String name, GetSchemaResponse schema) {
        return SchemaInfoImpl.builder()
                .name(name)
                .schema(schema.getData().getBytes(StandardCharsets.UTF_8))
                .type(schema.getType())
                .properties(schema.getProperties())
                .build();
    }
}
