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
import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.pulsar.client.api.schema.FieldSchemaBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * The default implementation of {@link FieldSchemaBuilder}.
 */
class FieldSchemaBuilderImpl implements FieldSchemaBuilder<FieldSchemaBuilderImpl> {

    private final String fieldName;

    private SchemaType type;
    private boolean optional = false;
    private Object defaultVal = null;
    private final Map<String, String> properties = new HashMap<>();
    private String doc;
    private String[] aliases;

    private GenericSchema genericSchema;

    FieldSchemaBuilderImpl(String fieldName) {
        this(fieldName, null);
    }

    FieldSchemaBuilderImpl(String fieldName, GenericSchema genericSchema) {
        this.fieldName = fieldName;
        this.genericSchema = genericSchema;
    }

    @Override
    public FieldSchemaBuilderImpl property(String name, String val) {
        properties.put(name, val);
        return this;
    }

    @Override
    public FieldSchemaBuilderImpl doc(String doc) {
        this.doc = doc;
        return this;
    }

    @Override
    public FieldSchemaBuilderImpl aliases(String... aliases) {
        this.aliases = aliases;
        return this;
    }

    @Override
    public FieldSchemaBuilderImpl type(SchemaType type) {
        this.type = type;
        return this;
    }

    @Override
    public FieldSchemaBuilderImpl optional() {
        optional = true;
        return this;
    }

    @Override
    public FieldSchemaBuilderImpl required() {
        optional = false;
        return this;
    }

    @Override
    public FieldSchemaBuilderImpl defaultValue(Object value) {
        defaultVal = value;
        return this;
    }

    Field build() {
        requireNonNull(type, "Schema type is not provided");
        // verify the default value and object
        SchemaUtils.validateFieldSchema(
            fieldName,
            type,
            defaultVal
        );

        final Schema baseSchema;
        switch (type) {
            case INT32:
                baseSchema = SchemaBuilder.builder().intType();
                break;
            case INT64:
                baseSchema = SchemaBuilder.builder().longType();
                break;
            case STRING:
                baseSchema = SchemaBuilder.builder().stringType();
                break;
            case FLOAT:
                baseSchema = SchemaBuilder.builder().floatType();
                break;
            case DOUBLE:
                baseSchema = SchemaBuilder.builder().doubleType();
                break;
            case BOOLEAN:
                baseSchema = SchemaBuilder.builder().booleanType();
                break;
            case BYTES:
                baseSchema = SchemaBuilder.builder().bytesType();
                break;
            // DATE, TIME, TIMESTAMP support from generic record
            case DATE:
                baseSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
                break;
            case TIME:
                baseSchema = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
                break;
            case TIMESTAMP:
                baseSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
                break;
            case JSON:
                checkArgument(genericSchema.getSchemaInfo().getType() == SchemaType.JSON,
                        "The field is expected to be using JSON schema but "
                                + genericSchema.getSchemaInfo().getType() + " schema is found");
                AvroBaseStructSchema genericJsonSchema = (AvroBaseStructSchema) genericSchema;
                baseSchema = genericJsonSchema.getAvroSchema();
                break;
            case AVRO:
                checkArgument(genericSchema.getSchemaInfo().getType() == SchemaType.AVRO,
                        "The field is expected to be using AVRO schema but "
                                + genericSchema.getSchemaInfo().getType() + " schema is found");
                GenericAvroSchema genericAvroSchema = (GenericAvroSchema) genericSchema;
                baseSchema = genericAvroSchema.getAvroSchema();
                break;
            default:
                throw new RuntimeException("Schema `" + type + "` is not supported to be used as a field for now");
        }

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            baseSchema.addProp(entry.getKey(), entry.getValue());
        }

        if (null != aliases) {
            for (String alias : aliases) {
                baseSchema.addAlias(alias);
            }
        }

        final Schema finalSchema;
        if (optional) {
            if (defaultVal != null) {
                finalSchema = SchemaBuilder.builder().unionOf()
                    .type(baseSchema)
                    .and()
                    .nullType()
                    .endUnion();
            } else {
                finalSchema = SchemaBuilder.builder().unionOf()
                    .nullType()
                    .and()
                    .type(baseSchema)
                    .endUnion();
            }
        } else {
            finalSchema = baseSchema;
        }

        final Object finalDefaultValue;
        if (defaultVal != null) {
            finalDefaultValue = SchemaUtils.toAvroObject(defaultVal);
        } else {
            if (optional) {
                finalDefaultValue = JsonProperties.NULL_VALUE;
            } else {
                finalDefaultValue = null;
            }
        }

        return new Field(
            fieldName,
            finalSchema,
            doc,
            finalDefaultValue
        );
    }

}
