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
     * Set schema whether always allow null or not.
     *
     * @param alwaysAllowNull definition null or not
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> withAlwaysAllowNull(boolean alwaysAllowNull);

    /**
     * Set schema use JRS310 conversion or not.
     * In prior versions of Pulsar, we supported joda time. However since that has been deprecated in favor
     * of Java time, we currently always use Java time. Thus this call has no impact. Just keeping here
     * for backwards compabtibility
     *
     * @param jsr310ConversionEnabled use JRS310 conversion or not, default is false for keep forwarding compatibility
     * @return schema definition builder
     */
    @Deprecated
    SchemaDefinitionBuilder<T> withJSR310ConversionEnabled(boolean jsr310ConversionEnabled);

    /**
     * Set schema info properties.
     *
     * @param properties schema info properties
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> withProperties(Map<String, String> properties);

    /**
     * Set schema info properties.
     *
     * @param key property key
     * @param value property value
     *
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> addProperty(String key, String value);

    /**
     * Set schema of pojo definition.
     *
     * @param pojo pojo schema definition
     *
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> withPojo(Class pojo);

    /**
     * Set schema of json definition.
     *
     * @param jsonDefinition json schema definition
     *
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> withJsonDef(String jsonDefinition);

    /**
     * Set schema whether decode by schema version.
     *
     * @param supportSchemaVersioning decode by version
     *
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> withSupportSchemaVersioning(boolean supportSchemaVersioning);

    /**
     * Set schema reader for deserialization of object data.
     *
     * @param reader reader for object data
     *
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> withSchemaReader(SchemaReader<T> reader);

    /**
     * Set schema writer for serialization of objects.
     *
     * @param writer writer for objects
     *
     * @return schema definition builder
     */
    SchemaDefinitionBuilder<T> withSchemaWriter(SchemaWriter<T> writer);

    /**
     * Build the schema definition.
     *
     * @return the schema definition.
     */
    SchemaDefinition<T> build();

}
