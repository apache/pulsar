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

import java.util.HashMap;
import java.util.Map;

/**
 * A schema definition
 * {@link org.apache.pulsar.client.api.Schema} for the schema definition value.
 */
@Data
@EqualsAndHashCode
@ToString
public class SchemaDefinition<T> {

    /**
     * the schema definition class
     */
    private final Class<T> clazz;
    /**
     * The flag of schema type always null
     */
    private boolean alwaysNull = true;
    /**
     * The schema info properties
     */
    private Map<String, String> properties = new HashMap<>();


    public SchemaDefinition(Class<T> clazz) {

        properties.put("alwaysNull", "true");
        this.clazz = clazz;

    }

    /**
     * Set schema whether always null or not
     *
     * @param alwaysNull definition null or not
     * @return record schema definition
     */
    public SchemaDefinition<T> alwaysNull(boolean alwaysNull) {

        this.alwaysNull = alwaysNull;
        properties.put("alwaysNull", alwaysNull ? "true" : "false");
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
     * Get schema class
     *
     * @return schema class
     */
    public Class<T> getClazz() {

        return clazz;
    }


    /**
     * Set schema info properties
     *
     * @param properties schema info properties
     * @return record schema definition
     */
    public SchemaDefinition<T> properties(Map<String, String> properties) {

        this.properties = properties;
        properties.put("alwaysNull", alwaysNull ? "true" : "false");
        return this;
    }
}
