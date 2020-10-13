package org.apache.pulsar.client.impl.schema.generic;

import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.AvroBasedStructSchema;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Abstract AvroBased GenericSchema
 */
abstract class AbstractAvroBasedGenericSchema extends AvroBasedStructSchema<GenericRecord> implements GenericSchema<GenericRecord> {

    protected final List<Field> fields;
    // the flag controls whether to use the provided schema as reader schema
    // to decode the messages. In `AUTO_CONSUME` mode, setting this flag to `false`
    // allows decoding the messages using the schema associated with the messages.
    protected final boolean useProvidedSchemaAsReaderSchema;

    protected AbstractAvroBasedGenericSchema(SchemaInfo schemaInfo, boolean useProvidedSchemaAsReaderSchema) {
        super(schemaInfo);
        this.fields = schema.getFields()
                .stream()
                .map(f -> new Field(f.name(), f.pos()))
                .collect(Collectors.toList());
        this.useProvidedSchemaAsReaderSchema = useProvidedSchemaAsReaderSchema;
    }

    @Override
    public List<Field> getFields() {
        return fields;
    }

    /**
     * Create a generic schema out of a <tt>SchemaInfo</tt>.
     *
     * @param schemaInfo schema info
     * @return a generic schema instance
     */
    public static GenericSchema of(SchemaInfo schemaInfo) {
        return of(schemaInfo, true);
    }

    public static GenericSchema of(SchemaInfo schemaInfo,
                                   boolean useProvidedSchemaAsReaderSchema) {
        switch (schemaInfo.getType()) {
            case AVRO:
                return new GenericAvroSchema(schemaInfo, useProvidedSchemaAsReaderSchema);
            case JSON:
                return new GenericJsonSchema(schemaInfo, useProvidedSchemaAsReaderSchema);
            default:
                throw new UnsupportedOperationException("AvroBased Generic schema is not supported on schema type "
                        + schemaInfo.getType() + "'");
        }
    }

}
