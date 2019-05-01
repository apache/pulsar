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

import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaDefinitionBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder to build {@link org.apache.pulsar.client.api.schema.GenericRecord}.
 */
public class SchemaDefinitionBuilderImpl<T> implements SchemaDefinitionBuilder<T> {

    public static final String ALWAYS_ALLOW_NULL = "__alwaysAllowNull";

    /**
     * the schema definition class
     */
    private  Class<T> clazz;
    /**
     * The flag of schema type always allow null
     *
     * If it's true, will make all of the pojo field generate schema
     * define default can be null,false default can't be null, but it's
     * false you can define the field by yourself by the annotation@Nullable
     *
     */
    private boolean alwaysAllowNull = true;

    /**
     * The schema info properties
     */
    private Map<String, String> properties = new HashMap<>();

    /**
     * The json schema definition
     */
    private String jsonDef;

    /**
     * The flag of message decode whether by schema version
     */
    private boolean supportSchemaVersioning = false;

    @Override
    public SchemaDefinitionBuilder<T> withAlwaysAllowNull(boolean alwaysAllowNull) {
        this.alwaysAllowNull = alwaysAllowNull;
        return this;
    }

    @Override
    public SchemaDefinitionBuilder<T> addProperty(String key, String value) {
        this.properties.put(key, value);
        return this;
    }

    @Override
    public SchemaDefinitionBuilder<T> withPojo(Class clazz) {
        this.clazz = clazz;
        return this;
    }

    @Override
    public SchemaDefinitionBuilder<T> withJsonDef(String jsonDef) {
        this.jsonDef = jsonDef;
        return this;
    }

    @Override
    public SchemaDefinitionBuilder<T> withSupportSchemaVersioning(boolean supportSchemaVersioning) {
        this.supportSchemaVersioning = supportSchemaVersioning;
        return this;
    }

    @Override
    public SchemaDefinitionBuilder<T> withProperties(Map<String,String> properties) {
        this.properties = properties;
        return this;
    }

    @Override
    public  SchemaDefinition<T> build() {
        properties.put(ALWAYS_ALLOW_NULL, this.alwaysAllowNull ? "true" : "false");
        return new SchemaDefinitionImpl(clazz, jsonDef, alwaysAllowNull, properties, supportSchemaVersioning);

    }
}
