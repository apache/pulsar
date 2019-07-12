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



import java.util.Map;

/**
 * Builder to build schema definition {@link SchemaDefinition}.
 */
public interface SchemaDefinitionBuilder<T> {

    /**
     * Set schema whether always allow null or not
     *
     * @param alwaysAllowNull definition null or not
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> withAlwaysAllowNull(boolean alwaysAllowNull);

    /**
     * Set schema info properties
     *
     * @param properties schema info properties
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> withProperties(Map<String, String> properties);

    /**
     * Set schema info properties
     *
     * @param key property key
     * @param value property value
     *
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> addProperty(String key, String value);

    /**
     * Set schema of pojo definition
     *
     * @param pojo pojo schema definition
     *
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> withPojo(Class pojo);

    /**
     * Set schema of json definition
     *
     * @param jsonDefinition json schema definition
     *
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> withJsonDef(String jsonDefinition);

    /**
     * Set schema whether decode by schema version
     *
     * @param supportSchemaVersioning decode by version
     *
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> withSupportSchemaVersioning(boolean supportSchemaVersioning);

    /**
     * Build the schema definition.
     *
     * @return the schema definition.
     */
    SchemaDefinition<T> build();

}
