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
package org.apache.pulsar.common.schema;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import lombok.experimental.Accessors;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.KeyValue;
import org.apache.pulsar.common.api.proto.PulsarApi.Schema;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Accessors(chain = true)
public class SchemaInfo {

    @EqualsAndHashCode.Exclude
    private String name;


    /**
     * The schema data in AVRO JSON format
     */
    private byte[] schema;

    /**
     * The type of schema (AVRO, JSON, PROTOBUF, etc..)
     */
    private SchemaType type;

    /**
     * Additional properties of the schema definition (implementation defined)
     */
    private Map<String, String> properties = Collections.emptyMap();

    public SchemaInfo(String name, SchemaData data) {
        this.name = name;
        this.schema = data.getData();
        this.type = data.getType();
        this.properties = data.getProps();
    }

    public SchemaInfo(Schema schema) {
        this.name = schema.getName();
        this.schema = schema.getSchemaData().toByteArray();
        this.type = Commands.getSchemaType(schema.getType());
        if (schema.getPropertiesCount() == 0) {
            this.properties = Collections.emptyMap();
        } else {
            this.properties = new TreeMap<>();
            for (int i =0 ; i < schema.getPropertiesCount();i ++ ) {
                KeyValue kv = schema.getProperties(i);
                properties.put(kv.getKey(), kv.getValue());
            }
        }
    }

    public SchemaInfo(String name, GetSchemaResponse schema) {
        this.name = name;
        this.schema = schema.getData().getBytes();
        this.type = schema.getType();
        this.properties = schema.getProperties();
    }
}
