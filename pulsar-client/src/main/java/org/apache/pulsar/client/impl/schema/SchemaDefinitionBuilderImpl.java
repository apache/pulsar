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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaDefinitionBuilder;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder to build {@link org.apache.pulsar.client.api.schema.GenericRecord}.
 */
public class SchemaDefinitionBuilderImpl<T> implements SchemaDefinitionBuilder<T> {

    public static final String ALWAYS_ALLOW_NULL = "__alwaysAllowNull";
    public static final String JSR310_CONVERSION_ENABLED = "__jsr310ConversionEnabled";

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
     * The flag of use JSR310 conversion or Joda time conversion.
     *
     * If value is true, use JSR310 conversion in the Avro schema. Otherwise, use Joda time conversion.
     */
    private boolean jsr310ConversionEnabled = false;

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

    private SchemaReader<T> reader;

    private SchemaWriter<T> writer;

    @Override
    public SchemaDefinitionBuilder<T> withAlwaysAllowNull(boolean alwaysAllowNull) {
        this.alwaysAllowNull = alwaysAllowNull;
        return this;
    }

    @Override
    public SchemaDefinitionBuilder<T> withJSR310ConversionEnabled(boolean jsr310ConversionEnabled) {
        this.jsr310ConversionEnabled = jsr310ConversionEnabled;
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
        if (properties.containsKey(ALWAYS_ALLOW_NULL)) {
            alwaysAllowNull = Boolean.parseBoolean(properties.get(ALWAYS_ALLOW_NULL));
        }
        if (properties.containsKey(ALWAYS_ALLOW_NULL)) {
            jsr310ConversionEnabled = Boolean.parseBoolean(properties.get(JSR310_CONVERSION_ENABLED));
        }
        return this;
    }

    @Override
    public SchemaDefinitionBuilder<T> withSchemaReader(SchemaReader<T> reader) {
        this.reader=reader;
        return this;
    }

    @Override
    public SchemaDefinitionBuilder<T> withSchemaWriter(SchemaWriter<T> writer) {
        this.writer = writer;
        return this;
    }

    @Override
    public  SchemaDefinition<T> build() {
        checkArgument(StringUtils.isNotBlank(jsonDef) || clazz != null,
                "Must specify one of the pojo or jsonDef for the schema definition.");

        checkArgument(!(StringUtils.isNotBlank(jsonDef) && clazz != null),
                "Not allowed to set pojo and jsonDef both for the schema definition.");

        checkArgument((reader != null && writer != null) || (reader == null && writer == null),
                "Must specify reader and writer or none of them.");

        properties.put(ALWAYS_ALLOW_NULL, String.valueOf(this.alwaysAllowNull));
        properties.put(JSR310_CONVERSION_ENABLED, String.valueOf(this.jsr310ConversionEnabled));
        return new SchemaDefinitionImpl(clazz, jsonDef, alwaysAllowNull, properties, supportSchemaVersioning,
                jsr310ConversionEnabled, reader, writer);

    }
}
