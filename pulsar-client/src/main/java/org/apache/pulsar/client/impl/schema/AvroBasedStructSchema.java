package org.apache.pulsar.client.impl.schema;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.lang.reflect.Field;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Avro Based StructSchema abstract class
 */
public abstract class AvroBasedStructSchema<T> extends StructSchema<T> {

    protected final Schema schema;

    protected AvroBasedStructSchema(SchemaInfo schemaInfo) {
        super(schemaInfo);
        this.schema = parseAvroSchema(new String(schemaInfo.getSchema(), UTF_8));
        this.schemaInfo = schemaInfo;
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
        return SchemaInfo.builder()
                .schema(createAvroSchema(schemaDefinition).toString().getBytes(UTF_8))
                .properties(schemaDefinition.getProperties())
                .name("")
                .type(schemaType).build();
    }

}
