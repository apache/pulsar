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

import static com.google.common.base.Preconditions.checkState;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.concurrent.ExecutionException;

/**
 * Auto detect schema.
 */
@Slf4j
public class AutoConsumeSchema implements Schema<GenericRecord> {

    private Schema<GenericRecord> schema;

    private String topicName;

    private String componentName;

    private SchemaInfoProvider schemaInfoProvider;

    public void setSchema(Schema<GenericRecord> schema) {
        this.schema = schema;
    }

    private void ensureSchemaInitialized() {
        checkState(null != schema, "Schema is not initialized before used");
    }

    @Override
    public void validate(byte[] message) {
        ensureSchemaInitialized();

        schema.validate(message);
    }

    @Override
    public boolean supportSchemaVersioning() {
        return true;
    }

    @Override
    public byte[] encode(GenericRecord message) {
        ensureSchemaInitialized();

        return schema.encode(message);
    }

    @Override
    public GenericRecord decode(byte[] bytes, byte[] schemaVersion) {
        if (schema == null) {
            SchemaInfo schemaInfo = null;
            try {
                schemaInfo = schemaInfoProvider.getLatestSchema().get();
            } catch (InterruptedException | ExecutionException e ) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                log.error("Con't get last schema for topic {} use AutoConsumeSchema", topicName);
                throw new SchemaSerializationException(e.getCause());
            }
            schema = generateSchema(schemaInfo);
            schema.setSchemaInfoProvider(schemaInfoProvider);
            log.info("Configure {} schema for topic {} : {}",
                    componentName, topicName, schemaInfo.getSchemaDefinition());
        }
        ensureSchemaInitialized();
        return schema.decode(bytes, schemaVersion);
    }

    @Override
    public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {
        if (schema == null) {
            this.schemaInfoProvider = schemaInfoProvider;
        } else {
            schema.setSchemaInfoProvider(schemaInfoProvider);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        if (schema == null) {
            return null;
        }
        return schema.getSchemaInfo();
    }

    @Override
    public boolean requireFetchingSchemaInfo() {
        return true;
    }

    @Override
    public void configureSchemaInfo(String topicName,
                                    String componentName,
                                    SchemaInfo schemaInfo) {
        this.topicName = topicName;
        this.componentName = componentName;
        if (schemaInfo != null) {
            GenericSchema genericSchema = generateSchema(schemaInfo);
            setSchema(genericSchema);
            log.info("Configure {} schema for topic {} : {}",
                    componentName, topicName, schemaInfo.getSchemaDefinition());
        }
    }

    @Override
    public Schema<GenericRecord> clone() {
        Schema<GenericRecord> schema = Schema.AUTO_CONSUME();
        if (this.schema != null) {
            schema.configureSchemaInfo(topicName, componentName, this.schema.getSchemaInfo());
        } else {
            schema.configureSchemaInfo(topicName, componentName, null);
        }
        if (schemaInfoProvider != null) {
            schema.setSchemaInfoProvider(schemaInfoProvider);
        }
        return schema;
    }

    private GenericSchema generateSchema(SchemaInfo schemaInfo) {
        if (schemaInfo.getType() != SchemaType.AVRO
            && schemaInfo.getType() != SchemaType.JSON) {
            throw new RuntimeException("Currently auto consume only works for topics with avro or json schemas");
        }
        // when using `AutoConsumeSchema`, we use the schema associated with the messages as schema reader
        // to decode the messages.
        return GenericSchemaImpl.of(schemaInfo, false /*useProvidedSchemaAsReaderSchema*/);
    }

    public static Schema<?> getSchema(SchemaInfo schemaInfo) {
        switch (schemaInfo.getType()) {
            case INT8:
                return ByteSchema.of();
            case INT16:
                return ShortSchema.of();
            case INT32:
                return IntSchema.of();
            case INT64:
                return LongSchema.of();
            case STRING:
                return StringSchema.utf8();
            case FLOAT:
                return FloatSchema.of();
            case DOUBLE:
                return DoubleSchema.of();
            case BOOLEAN:
                return BooleanSchema.of();
            case BYTES:
                return BytesSchema.of();
            case DATE:
                return DateSchema.of();
            case TIME:
                return TimeSchema.of();
            case TIMESTAMP:
                return TimestampSchema.of();
            case INSTANT:
                return InstantSchema.of();
            case LOCAL_DATE:
                return LocalDateSchema.of();
            case LOCAL_TIME:
                return LocalTimeSchema.of();
            case LOCAL_DATE_TIME:
                return LocalDateTimeSchema.of();
            case JSON:
            case AVRO:
                return GenericSchemaImpl.of(schemaInfo);
            case KEY_VALUE:
                KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo =
                    KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
                Schema<?> keySchema = getSchema(kvSchemaInfo.getKey());
                Schema<?> valueSchema = getSchema(kvSchemaInfo.getValue());
                return KeyValueSchema.of(keySchema, valueSchema);
            default:
                throw new IllegalArgumentException("Retrieve schema instance from schema info for type '"
                    + schemaInfo.getType() + "' is not supported yet");
        }
    }
}
