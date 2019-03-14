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

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaDefinitionBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * A schema definition
 * {@link org.apache.pulsar.client.api.Schema} for the schema definition value.
 */
public class SchemaDefinitionImpl<T> implements SchemaDefinition<T>{

    private  final Class<T> clazz;

    private boolean alwaysAllowNull;

    private Map<String, String> properties;

    public SchemaDefinitionImpl(Class<T> clazz, boolean alwaysAllowNull, Map<String,String> properties) {
        this.alwaysAllowNull = alwaysAllowNull;
        this.properties = properties;
        this.clazz = clazz;
    }
    /**
     * get schema whether always allow null or not
     *
     * @return schema always null or not
     */
    public boolean getAlwaysAllowNull() {

        return alwaysAllowNull;
    }

    /**
     * Get schema class
     *
     * @return schema class
     */
    public Class<T> getClazz() {

        return clazz;
    }
    /**
     * Get schema class
     *
     * @return schema class
     */
    public Map<String, String> getProperties() {

        return properties;
    }



}
