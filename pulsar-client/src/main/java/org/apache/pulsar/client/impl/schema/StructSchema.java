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
import java.lang.reflect.Field;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * This is a base schema implementation for Avro Based `Struct` types.
 * A struct type is used for presenting records (objects) which
 * have multiple fields.
 *
 * <p>Currently Pulsar supports 3 `Struct` types -
 * {@link org.apache.pulsar.common.schema.SchemaType#AVRO},
 * {@link org.apache.pulsar.common.schema.SchemaType#JSON},
 * and {@link org.apache.pulsar.common.schema.SchemaType#PROTOBUF}.
 */
@Deprecated
public abstract class StructSchema<T> extends AbstractStructSchema<T> {

    protected final Schema schema;

    protected StructSchema(SchemaInfo schemaInfo) {
        super(schemaInfo);
        this.schema = parseAvroSchema(new String(schemaInfo.getSchema(), UTF_8));
        if (schemaInfo.getProperties().containsKey(GenericAvroSchema.OFFSET_PROP)) {
            this.schema.addProp(GenericAvroSchema.OFFSET_PROP,
                    schemaInfo.getProperties().get(GenericAvroSchema.OFFSET_PROP));
        }
    }

    public Schema getAvroSchema() {
        return schema;
    }


    protected static Schema createAvroSchema(SchemaDefinition schemaDefinition) {
        Class pojo = schemaDefinition.getPojo();

        if (StringUtils.isNotBlank(schemaDefinition.getJsonDef())) {
            return parseAvroSchema(schemaDefinition.getJsonDef());
        } else if (pojo != null) {
            ThreadLocal<Boolean> validateDefaults = null;

            try {
                Field validateDefaultsField = Schema.class.getDeclaredField("VALIDATE_DEFAULTS");
                validateDefaultsField.setAccessible(true);
                validateDefaults = (ThreadLocal<Boolean>) validateDefaultsField.get(null);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException("Cannot disable validation of default values", e);
            }

            final boolean savedValidateDefaults = validateDefaults.get();

            try {
                // Disable validation of default values for compatibility
                validateDefaults.set(false);
                return extractAvroSchema(schemaDefinition, pojo);
            } finally {
                validateDefaults.set(savedValidateDefaults);
            }
        } else {
            throw new RuntimeException("Schema definition must specify pojo class or schema json definition");
        }
    }

    protected static Schema extractAvroSchema(SchemaDefinition schemaDefinition, Class pojo) {
        try {
            return parseAvroSchema(pojo.getDeclaredField("SCHEMA$").get(null).toString());
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException ignored) {
            return schemaDefinition.getAlwaysAllowNull() ? ReflectData.AllowNull.get().getSchema(pojo)
                    : ReflectData.get().getSchema(pojo);
        }
    }

    protected static Schema parseAvroSchema(String schemaJson) {
        final Schema.Parser parser = new Schema.Parser();
        parser.setValidateDefaults(false);
        return parser.parse(schemaJson);
    }

    public static <T> SchemaInfo parseSchemaInfo(SchemaDefinition<T> schemaDefinition, SchemaType schemaType) {
        return SchemaInfoImpl.builder()
                .schema(createAvroSchema(schemaDefinition).toString().getBytes(UTF_8))
                .properties(schemaDefinition.getProperties())
                .name("")
                .type(schemaType).build();
    }

}
