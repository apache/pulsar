package org.apache.pulsar.functions.transforms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;

@Slf4j
@Data
public class TransformContext {
    private Context context;
    private Schema<?> keySchema;
    private Object keyObject;
    private boolean keyModified;
    private Schema<?> valueSchema;
    private Object valueObject;
    private boolean valueModified;
    private KeyValueEncodingType keyValueEncodingType;
    private String key;
    private Map<String, String> properties;
    private String outputTopic;

    public TransformContext(Context context, Object value) {
        Record<?> currentRecord = context.getCurrentRecord();
        this.context = context;
        this.outputTopic = context.getOutputTopic();
        Schema<?> schema = context.getCurrentRecord().getSchema();
        if (schema instanceof KeyValueSchema && value instanceof KeyValue) {
            KeyValueSchema kvSchema = (KeyValueSchema) schema;
            KeyValue kv = (KeyValue) value;
            this.keySchema = kvSchema.getKeySchema();
            if (this.keySchema.getSchemaInfo().getType() == SchemaType.AVRO) {
                this.keyObject = ((org.apache.pulsar.client.api.schema.GenericRecord) kv.getKey()).getNativeObject();
            } else {
                this.keyObject = kv.getKey();
            }
            this.valueSchema = kvSchema.getValueSchema();
            if (this.valueSchema.getSchemaInfo().getType() == SchemaType.AVRO) {
                this.valueObject =
                        ((org.apache.pulsar.client.api.schema.GenericRecord) kv.getValue()).getNativeObject();
            } else {
                this.valueObject = kv.getValue();
            }
            this.keyValueEncodingType = kvSchema.getKeyValueEncodingType();
        } else {
            this.valueSchema = schema;
            this.valueObject = value;
            this.key = currentRecord.getKey().orElse(null);
            //TODO: should we make a copy ?
            this.properties = currentRecord.getProperties();
        }
    }

    public void send() throws IOException {
        if (keyModified && keySchema.getSchemaInfo().getType() == SchemaType.AVRO) {
            GenericRecord genericRecord = (GenericRecord) keyObject;
            keySchema = Schema.NATIVE_AVRO(genericRecord.getSchema());
            keyObject = serializeGenericRecord(genericRecord);
        }
        if (valueModified && valueSchema.getSchemaInfo().getType() == SchemaType.AVRO) {
            GenericRecord genericRecord = (GenericRecord) valueObject;
            valueSchema = Schema.NATIVE_AVRO(genericRecord.getSchema());
            valueObject = serializeGenericRecord(genericRecord);
        }

        Schema outputSchema;
        Object outputObject;
        if (keySchema != null) {
            outputSchema = Schema.KeyValue(keySchema, valueSchema, keyValueEncodingType);
            outputObject = new KeyValue(keyObject, valueObject);
        } else {
            outputSchema = valueSchema;
            // GenericRecord must stay wrapped while primitives must be unwrapped
            outputObject = !valueModified && valueSchema.getSchemaInfo().getType().isStruct()
                    ? context.getCurrentRecord().getValue()
                    : valueObject;
        }

        if (log.isDebugEnabled()) {
            log.debug("output {} schema {}", outputObject, outputSchema);
        }
        TypedMessageBuilder<?> message = context.newOutputMessage(outputTopic, outputSchema)
                .properties(properties)
                .value(outputObject);
        if (key != null) {
            message.key(key);
        }
        message.send();
    }

    public static byte[] serializeGenericRecord(GenericRecord record) throws IOException {
        GenericDatumWriter writer = new GenericDatumWriter(record.getSchema());
        ByteArrayOutputStream oo = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(oo, null);
        writer.write(record, encoder);
        return oo.toByteArray();
    }
}
