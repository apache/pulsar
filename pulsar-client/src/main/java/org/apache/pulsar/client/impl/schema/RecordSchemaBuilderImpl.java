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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.schema.FieldSchemaBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * The default implementation of {@link RecordSchemaBuilder}.
 */
public class RecordSchemaBuilderImpl implements RecordSchemaBuilder {

    public static final String NAMESPACE = "org.apache.pulsar.schema.record";
    public static final String DEFAULT_SCHEMA_NAME = "PulsarDefault";

    private final String name;
    private final Map<String, String> properties;
    private final List<FieldSchemaBuilderImpl> fields = new ArrayList<>();
    private String doc;

    public RecordSchemaBuilderImpl(String name) {
        this.name = name;
        this.properties = new HashMap<>();
    }

    @Override
    public RecordSchemaBuilder property(String name, String val) {
        this.properties.put(name, val);
        return this;
    }

    @Override
    public FieldSchemaBuilder field(String fieldName) {
        FieldSchemaBuilderImpl field = new FieldSchemaBuilderImpl(fieldName);
        fields.add(field);
        return field;
    }

    @Override
    public FieldSchemaBuilder field(String fieldName, GenericSchema genericSchema) {
        FieldSchemaBuilderImpl field = new FieldSchemaBuilderImpl(fieldName, genericSchema);
        fields.add(field);
        return field;
    }

    @Override
    public RecordSchemaBuilder doc(String doc) {
        this.doc = doc;
        return this;
    }

    @Override
    public SchemaInfo build(SchemaType schemaType) {
        switch (schemaType) {
            case JSON:
            case AVRO:
                break;
            default:
                throw new RuntimeException("Currently only AVRO and JSON record schema is supported");
        }

        String schemaNs = NAMESPACE;
        String schemaName = DEFAULT_SCHEMA_NAME;
        if (name != null) {
            String[] split = splitName(name);
            schemaNs = split[0];
            schemaName = split[1];
        }

        org.apache.avro.Schema baseSchema = org.apache.avro.Schema.createRecord(
            schemaName != null ? schemaName : DEFAULT_SCHEMA_NAME,
            doc,
            schemaNs,
            false
        );

        List<org.apache.avro.Schema.Field> avroFields = new ArrayList<>();
        for (FieldSchemaBuilderImpl field : fields) {
            avroFields.add(field.build());
        }

        baseSchema.setFields(avroFields);
        return new SchemaInfoImpl(
            name,
            baseSchema.toString().getBytes(UTF_8),
            schemaType,
            properties
        );
    }

    /**
     * Split a full dotted-syntax name into a namespace and a single-component name.
     */
    private static String[] splitName(String fullName) {
        String[] result = new String[2];
        int indexLastDot = fullName.lastIndexOf('.');
        if (indexLastDot >= 0) {
            result[0] = fullName.substring(0, indexLastDot);
            result[1] = fullName.substring(indexLastDot + 1);
        } else {
            result[0] = null;
            result[1] = fullName;
        }
        return result;
    }

}
