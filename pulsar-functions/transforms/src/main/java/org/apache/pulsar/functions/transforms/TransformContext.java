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
import org.apache.pulsar.client.api.schema.GenericObject;
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
        Schema<?> schema = currentRecord.getSchema();
        //TODO: should we make a copy ?
        this.properties = currentRecord.getProperties();
        if (schema instanceof KeyValueSchema && value instanceof KeyValue) {
            KeyValueSchema kvSchema = (KeyValueSchema) schema;
            KeyValue kv = (KeyValue) value;
            this.keySchema = kvSchema.getKeySchema();
            this.keyObject = this.keySchema.getSchemaInfo().getType() == SchemaType.AVRO
                    ? ((org.apache.pulsar.client.api.schema.GenericRecord) kv.getKey()).getNativeObject()
                    : kv.getKey();
            this.valueSchema = kvSchema.getValueSchema();
            this.valueObject = this.valueSchema.getSchemaInfo().getType() == SchemaType.AVRO
                    ? ((org.apache.pulsar.client.api.schema.GenericRecord) kv.getValue()).getNativeObject()
                    : kv.getValue();
            this.keyValueEncodingType = kvSchema.getKeyValueEncodingType();
        } else {
            this.valueSchema = schema;
            this.valueObject = value;
            this.key = currentRecord.getKey().orElse(null);
        }
    }

    public void send() throws IOException {
        if (keyModified && keySchema != null && keySchema.getSchemaInfo().getType() == SchemaType.AVRO) {
            GenericRecord genericRecord = (GenericRecord) keyObject;
            keySchema = Schema.NATIVE_AVRO(genericRecord.getSchema());
            keyObject = serializeGenericRecord(genericRecord);
        }
        if (valueModified && valueSchema != null && valueSchema.getSchemaInfo().getType() == SchemaType.AVRO) {
            GenericRecord genericRecord = (GenericRecord) valueObject;
            valueSchema = Schema.NATIVE_AVRO(genericRecord.getSchema());
            valueObject = serializeGenericRecord(genericRecord);
        }

        Schema outputSchema;
        Object outputObject;
        GenericObject recordValue = (GenericObject) context.getCurrentRecord().getValue();
        if (keySchema != null) {
            outputSchema = Schema.KeyValue(keySchema, valueSchema, keyValueEncodingType);
            Object outputKeyObject = !keyModified && keySchema.getSchemaInfo().getType().isStruct()
                    ? ((KeyValue) recordValue.getNativeObject()).getKey()
                    : keyObject;
            Object outputValueObject = !valueModified && valueSchema.getSchemaInfo().getType().isStruct()
                    ? ((KeyValue) recordValue.getNativeObject()).getValue()
                    : valueObject;
            outputObject = new KeyValue(outputKeyObject, outputValueObject);
        } else {
            outputSchema = valueSchema;
            outputObject = !valueModified && valueSchema.getSchemaInfo().getType().isStruct()
                    ? recordValue
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
