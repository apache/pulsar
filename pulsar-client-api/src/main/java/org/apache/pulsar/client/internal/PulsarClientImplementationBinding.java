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
package org.apache.pulsar.client.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessagePayloadFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaDefinitionBuilder;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Helper class for class instantiations and it also contains methods to work with schemas.
 * This interface allows you to not depend on the Implementation classes directly.
 * The actual implementation of this class is loaded from {@link DefaultImplementation}.
 */
public interface PulsarClientImplementationBinding {
    <T> SchemaDefinitionBuilder<T> newSchemaDefinitionBuilder();

    ClientBuilder newClientBuilder();

    MessageId newMessageId(long ledgerId, long entryId, int partitionIndex);

    MessageId newMessageIdFromByteArray(byte[] data) throws IOException;

    MessageId newMessageIdFromByteArrayWithTopic(byte[] data, String topicName) throws IOException;

    Authentication newAuthenticationToken(String token);

    Authentication newAuthenticationToken(Supplier<String> supplier);

    Authentication newAuthenticationTLS(String certFilePath, String keyFilePath);

    Authentication createAuthentication(String authPluginClassName, String authParamsString)
            throws PulsarClientException.UnsupportedAuthenticationException;

    Authentication createAuthentication(String authPluginClassName, Map<String, String> authParams)
            throws PulsarClientException.UnsupportedAuthenticationException;

    Schema<byte[]> newBytesSchema();

    Schema<String> newStringSchema();

    Schema<String> newStringSchema(Charset charset);

    Schema<Byte> newByteSchema();

    Schema<Short> newShortSchema();

    Schema<Integer> newIntSchema();

    Schema<Long> newLongSchema();

    Schema<Boolean> newBooleanSchema();

    Schema<ByteBuffer> newByteBufferSchema();

    Schema<Float> newFloatSchema();

    Schema<Double> newDoubleSchema();

    Schema<Date> newDateSchema();

    Schema<Time> newTimeSchema();

    Schema<Timestamp> newTimestampSchema();

    Schema<Instant> newInstantSchema();

    Schema<LocalDate> newLocalDateSchema();

    Schema<LocalTime> newLocalTimeSchema();

    Schema<LocalDateTime> newLocalDateTimeSchema();

    <T> Schema<T> newAvroSchema(SchemaDefinition schemaDefinition);

    <T extends com.google.protobuf.GeneratedMessageV3> Schema<T> newProtobufSchema(SchemaDefinition schemaDefinition);

    <T extends com.google.protobuf.GeneratedMessageV3> Schema<T> newProtobufNativeSchema(
            SchemaDefinition schemaDefinition);

    <T> Schema<T> newJSONSchema(SchemaDefinition schemaDefinition);

    Schema<GenericRecord> newAutoConsumeSchema();

    Schema<byte[]> newAutoProduceSchema();

    Schema<byte[]> newAutoProduceSchema(Schema<?> schema);

    Schema<byte[]> newAutoProduceValidatedAvroSchema(Object schema);

    Schema<KeyValue<byte[], byte[]>> newKeyValueBytesSchema();

    <K, V> Schema<KeyValue<K, V>> newKeyValueSchema(Schema<K> keySchema, Schema<V> valueSchema,
                                                           KeyValueEncodingType keyValueEncodingType);

    <K, V> Schema<KeyValue<K, V>> newKeyValueSchema(Class<K> key, Class<V> value, SchemaType type);

    Schema<?> getSchema(SchemaInfo schemaInfo);

    GenericSchema<GenericRecord> getGenericSchema(SchemaInfo schemaInfo);

    RecordSchemaBuilder newRecordSchemaBuilder(String name);

    /**
     * Decode the kv encoding type from the schema info.
     *
     * @param schemaInfo the schema info
     * @return the kv encoding type
     */
    KeyValueEncodingType decodeKeyValueEncodingType(SchemaInfo schemaInfo);

    /**
     * Encode key & value into schema into a KeyValue schema.
     *
     * @param keySchema            the key schema
     * @param valueSchema          the value schema
     * @param keyValueEncodingType the encoding type to encode and decode key value pair
     * @return the final schema info
     */
    <K, V> SchemaInfo encodeKeyValueSchemaInfo(Schema<K> keySchema,
                                                      Schema<V> valueSchema,
                                                      KeyValueEncodingType keyValueEncodingType);

    /**
     * Encode key & value into schema into a KeyValue schema.
     *
     * @param schemaName           the final schema name
     * @param keySchema            the key schema
     * @param valueSchema          the value schema
     * @param keyValueEncodingType the encoding type to encode and decode key value pair
     * @return the final schema info
     */
    <K, V> SchemaInfo encodeKeyValueSchemaInfo(String schemaName,
                                                      Schema<K> keySchema,
                                                      Schema<V> valueSchema,
                                                      KeyValueEncodingType keyValueEncodingType);

    /**
     * Decode the key/value schema info to get key schema info and value schema info.
     *
     * @param schemaInfo key/value schema info.
     * @return the pair of key schema info and value schema info
     */
    KeyValue<SchemaInfo, SchemaInfo> decodeKeyValueSchemaInfo(SchemaInfo schemaInfo);

    /**
     * Jsonify the schema info.
     *
     * @param schemaInfo the schema info
     * @return the jsonified schema info
     */
    String jsonifySchemaInfo(SchemaInfo schemaInfo);

    /**
     * Jsonify the schema info with version.
     *
     * @param schemaInfoWithVersion the schema info with version
     * @return the jsonified schema info with version
     */
    String jsonifySchemaInfoWithVersion(SchemaInfoWithVersion schemaInfoWithVersion);

    /**
     * Jsonify the key/value schema info.
     *
     * @param kvSchemaInfo the key/value schema info
     * @return the jsonified schema info
     */
    String jsonifyKeyValueSchemaInfo(KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo);

    /**
     * Convert the key/value schema data.
     *
     * @param kvSchemaInfo the key/value schema info
     * @return the convert key/value schema data string
     */
    String convertKeyValueSchemaInfoDataToString(KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo) throws IOException;

    /**
     * Convert the key/value schema info data json bytes to key/value schema info data bytes.
     *
     * @param keyValueSchemaInfoDataJsonBytes the key/value schema info data json bytes
     * @return the key/value schema info data bytes
     */
    byte[] convertKeyValueDataStringToSchemaInfoSchema(byte[] keyValueSchemaInfoDataJsonBytes) throws IOException;

    BatcherBuilder newDefaultBatcherBuilder();

    BatcherBuilder newKeyBasedBatcherBuilder();

    MessagePayloadFactory newDefaultMessagePayloadFactory();

    /**
     * Retrieves ByteBuffer data into byte[].
     *
     * @param byteBuffer
     * @return
     */
    static byte[] getBytes(ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return null;
        }
        if (byteBuffer.hasArray() && byteBuffer.arrayOffset() == 0
                && byteBuffer.array().length == byteBuffer.remaining()) {
            return byteBuffer.array();
        }
        // Direct buffer is not backed by array and it needs to be read from direct memory
        byte[] array = new byte[byteBuffer.remaining()];
        byteBuffer.get(array);
        return array;
    }

    SchemaInfo newSchemaInfoImpl(String name, byte[] schema, SchemaType type, long timestamp,
                                 Map<String, String> propertiesValue);
}
