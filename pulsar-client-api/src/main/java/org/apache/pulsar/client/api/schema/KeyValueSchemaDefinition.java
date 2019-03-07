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
package org.apache.pulsar.client.api.schema;


import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.HashMap;
import java.util.Map;

/**
 * A key and value schema definition
 * {@link org.apache.pulsar.client.api.Schema} for the key and value schema definition value.
 */
@Data
@EqualsAndHashCode
@ToString
public class KeyValueSchemaDefinition<K, V> {

    private Class<K> key;

    private Class<V> value;

    private boolean alwaysNull = true;

    private SchemaType type;

    private Schema<K> keySchema;

    private Schema<V> valueSchema;

    private Map<String, String> properties = new HashMap<>();

    public KeyValueSchemaDefinition() {

    }

    public KeyValueSchemaDefinition(Schema<K> keySchema, Schema<V> valueSchema) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    public KeyValueSchemaDefinition(Class<K> key, Class<V> value, SchemaType type) {
        this.key = key;
        this.value = value;
        this.type = type;
    }

    public KeyValueSchemaDefinition(Class<K> key, Class<V> value, boolean allowNull) {
        this.key = key;
        this.value = value;
        this.alwaysNull = allowNull;
        this.type = SchemaType.JSON;
    }

    /**
     * Get key schema class
     *
     * @return key schema class
     */
    public Class<K> getKey() {

        return key;
    }

    /**
     * Get value schema class
     *
     * @return value schema class
     */
    public Class<V> getValue() {

        return value;
    }

    /**
     * Set schema whether always null or not
     *
     * @param alwaysNull definition null or not
     * @return Key and value schema definition
     */
    public KeyValueSchemaDefinition<K, V> alwaysNull(boolean alwaysNull) {

        this.alwaysNull = alwaysNull;
        properties.put("alwaysNull", alwaysNull ? "true" : "false");
        return this;
    }

    /**
     * Set key schema
     *
     * @param keySchema definition key schema
     * @return Key and value schema definition
     */
    public KeyValueSchemaDefinition<K, V> keySchema(Schema<K> keySchema) {

        this.keySchema = keySchema;
        return this;
    }

    /**
     * Set value schema
     *
     * @param valueSchema definition null or not
     * @return Key and value schema definition
     */
    public KeyValueSchemaDefinition<K, V> valueSchema(Schema<V> valueSchema) {

        this.valueSchema = valueSchema;
        return this;
    }

    /**
     * get schema whether always null or not
     *
     * @return schema always null or not
     */
    public boolean getAlwaysNull() {

        return alwaysNull;
    }

    /**
     * Set schema type
     *
     * @param type the schema type
     * @return Key and value schema definition
     */
    public KeyValueSchemaDefinition<K, V> type(SchemaType type) {

        this.type = type;
        return this;
    }

    /**
     * Get schema type
     *
     * @return Schema type
     */
    public SchemaType getType() {

        return type;
    }

    /**
     * Set schema info properties
     *
     * @param properties schema info properties
     * @return record schema definition
     */
    public KeyValueSchemaDefinition<K, V> properties(Map<String, String> properties) {

        this.properties = properties;
        properties.put("alwaysNull", alwaysNull ? "true" : "false");
        return this;
    }
}
