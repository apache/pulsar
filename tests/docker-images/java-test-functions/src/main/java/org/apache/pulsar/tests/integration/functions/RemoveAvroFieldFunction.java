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
package org.apache.pulsar.tests.integration.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

import java.io.ByteArrayOutputStream;
import java.util.stream.Collectors;

/**
 * This function removes a "field" from a AVRO message
 */
@Slf4j
public class RemoveAvroFieldFunction implements Function<GenericObject, Void> {

    private static final String FIELD_TO_REMOVE = "age";

    @Override
    public Void process(GenericObject genericObject, Context context) throws Exception {
        Record<?> currentRecord = context.getCurrentRecord();
        log.info("apply to {} {}", genericObject, genericObject.getNativeObject());
        log.info("record with schema {} version {} {}", currentRecord.getSchema(),
                currentRecord.getMessage().get().getSchemaVersion(),
                currentRecord);
        Object nativeObject = genericObject.getNativeObject();
        Schema<?> schema = currentRecord.getSchema();

        Schema outputSchema = schema;
        Object outputObject = genericObject.getNativeObject();
        boolean someThingDone = false;
        if (schema instanceof KeyValueSchema && nativeObject instanceof KeyValue)  {
            KeyValueSchema kvSchema = (KeyValueSchema) schema;

            Schema keySchema = kvSchema.getKeySchema();
            Schema valueSchema = kvSchema.getValueSchema();
            // remove a column "age" from the "valueSchema"
            if (valueSchema.getSchemaInfo().getType() == SchemaType.AVRO) {

                org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) valueSchema.getNativeSchema().get();
                if (avroSchema.getField(FIELD_TO_REMOVE) != null) {
                    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
                    org.apache.avro.Schema originalAvroSchema = parser.parse(avroSchema.toString(false));
                    org.apache.avro.Schema modified = org.apache.avro.Schema.createRecord(
                            originalAvroSchema.getName(), originalAvroSchema.getDoc(), originalAvroSchema.getNamespace(), originalAvroSchema.isError(),
                            originalAvroSchema.getFields().
                                    stream()
                                    .filter(f->!f.name().equals(FIELD_TO_REMOVE))
                                    .map(f-> new org.apache.avro.Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()))
                                    .collect(Collectors.toList()));

                    KeyValue originalObject = (KeyValue) nativeObject;

                    GenericRecord value = (GenericRecord) originalObject.getValue();
                    org.apache.avro.generic.GenericRecord genericRecord
                            = (org.apache.avro.generic.GenericRecord) value.getNativeObject();

                    org.apache.avro.generic.GenericRecord newRecord = new GenericData.Record(modified);
                    for (org.apache.avro.Schema.Field field : modified.getFields()) {
                        newRecord.put(field.name(), genericRecord.get(field.name()));
                    }
                    GenericDatumWriter writer = new GenericDatumWriter(modified);
                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(oo, null);
                    writer.write(newRecord, encoder);
                    Object newValue = oo.toByteArray();

                    Schema newValueSchema = Schema.NATIVE_AVRO(modified);
                    outputSchema = Schema.KeyValue(keySchema, newValueSchema, kvSchema.getKeyValueEncodingType());
                    outputObject = new KeyValue(originalObject.getKey(), newValue);
                    someThingDone = true;
                }
            }
        } else if (schema.getSchemaInfo().getType() == SchemaType.AVRO) {
            org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) schema.getNativeSchema().get();
            if (avroSchema.getField(FIELD_TO_REMOVE) != null) {
                org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
                org.apache.avro.Schema originalAvroSchema = parser.parse(avroSchema.toString(false));
                org.apache.avro.Schema modified = org.apache.avro.Schema.createRecord(
                        originalAvroSchema.getName(), originalAvroSchema.getDoc(), originalAvroSchema.getNamespace(), originalAvroSchema.isError(),
                        originalAvroSchema.getFields().
                                stream()
                                .filter(f -> !f.name().equals(FIELD_TO_REMOVE))
                                .map(f -> new org.apache.avro.Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order()))
                                .collect(Collectors.toList()));

                org.apache.avro.generic.GenericRecord genericRecord
                        = (org.apache.avro.generic.GenericRecord) nativeObject;
                org.apache.avro.generic.GenericRecord newRecord = new GenericData.Record(modified);
                for (org.apache.avro.Schema.Field field : modified.getFields()) {
                    newRecord.put(field.name(), genericRecord.get(field.name()));
                }
                GenericDatumWriter writer = new GenericDatumWriter(modified);
                ByteArrayOutputStream oo = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(oo, null);
                writer.write(newRecord, encoder);

                Schema newValueSchema = Schema.NATIVE_AVRO(modified);
                outputSchema = newValueSchema;
                outputObject = oo.toByteArray();
                someThingDone = true;
            }
        }

        if (!someThingDone) {
            // do some processing...
            final boolean isStruct;
            switch (currentRecord.getSchema().getSchemaInfo().getType()) {
                case AVRO:
                case JSON:
                case PROTOBUF_NATIVE:
                    isStruct = true;
                    break;
                default:
                    isStruct = false;
                    break;
            }
            if (isStruct) {
                // GenericRecord must stay wrapped
                outputObject = currentRecord.getValue();
            } else {
                // primitives and KeyValue must be unwrapped
                outputObject = nativeObject;
            }
        }
        log.info("output {} schema {}", outputObject, outputSchema);
        context.newOutputMessage(context.getOutputTopic(), outputSchema)
                .value(outputObject).send();
        return null;
    }
}
