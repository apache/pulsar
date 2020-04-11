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

import java.util.Collections;
import java.util.Map;

/**
 * A json schema definition
 * {@link org.apache.pulsar.client.api.schema.SchemaDefinition} for the json schema definition.
 */
public class SchemaDefinitionImpl<T> implements SchemaDefinition<T>{

    /**
     * the schema definition class
     */
    private  Class<T> pojo;
    /**
     * The flag of schema type always allow null
     *
     * If it's true, will make all of the pojo field generate schema
     * define default can be null,false default can't be null, but it's
     * false you can define the field by yourself by the annotation@Nullable
     *
     */
    private final boolean alwaysAllowNull;

    private final Map<String, String> properties;

    private final String jsonDef;

    private final boolean supportSchemaVersioning;

    private final boolean jsr310ConversionEnabled;

    public SchemaDefinitionImpl(Class<T> pojo, String jsonDef, boolean alwaysAllowNull, Map<String,String> properties,
        boolean supportSchemaVersioning, boolean jsr310ConversionEnabled) {
        this.alwaysAllowNull = alwaysAllowNull;
        this.properties = properties;
        this.jsonDef = jsonDef;
        this.pojo = pojo;
        this.supportSchemaVersioning = supportSchemaVersioning;
        this.jsr310ConversionEnabled = jsr310ConversionEnabled;
    }
    /**
     * get schema whether always allow null or not
     *
     * @return schema always null or not
     */
    public boolean getAlwaysAllowNull() {
        return alwaysAllowNull;
    }

    @Override
    public boolean isJsr310ConversionEnabled() {
        return jsr310ConversionEnabled;
    }

    /**
     * Get json schema definition
     *
     * @return schema class
     */
    public String getJsonDef() {
        return jsonDef;
    }
    /**
     * Get pojo schema definition
     *
     * @return pojo class
     */
    @Override
    public Class<T> getPojo() {
        return pojo;
    }

    @Override
    public boolean getSupportSchemaVersioning() {
        return supportSchemaVersioning;
    }

    /**
     * Get schema class
     *
     * @return schema class
     */
    public Map<String, String> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

}
