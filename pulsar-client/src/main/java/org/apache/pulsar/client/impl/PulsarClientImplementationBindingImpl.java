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
package org.apache.pulsar.client.impl;


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
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaDefinitionBuilder;
import org.apache.pulsar.client.internal.PulsarClientImplementationBinding;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Helper class for class instantiations and it also contains methods to work with schemas.
 */
@SuppressWarnings("unchecked")
public class PulsarClientImplementationBindingImpl implements PulsarClientImplementationBinding {

    public <T> SchemaDefinitionBuilder<T> newSchemaDefinitionBuilder() {
        return new org.apache.pulsar.client.impl.schema.SchemaDefinitionBuilderImpl();
    }

    public ClientBuilder newClientBuilder() {
        return new org.apache.pulsar.client.impl.ClientBuilderImpl();
    }

    public MessageId newMessageId(long ledgerId, long entryId, int partitionIndex) {
        return new org.apache.pulsar.client.impl.MessageIdImpl(ledgerId, entryId, partitionIndex);
    }

    public MessageId newMessageIdFromByteArray(byte[] data) throws IOException {
        return org.apache.pulsar.client.impl.MessageIdImpl.fromByteArray(data);
    }

    public MessageId newMessageIdFromByteArrayWithTopic(byte[] data, String topicName) throws IOException {
        return org.apache.pulsar.client.impl.MessageIdImpl.fromByteArrayWithTopic(data, topicName);
    }

    public Authentication newAuthenticationToken(String token) {
        return new org.apache.pulsar.client.impl.auth.AuthenticationToken(token);
    }

    public Authentication newAuthenticationToken(Supplier<String> supplier) {
        return new org.apache.pulsar.client.impl.auth.AuthenticationToken(supplier);
    }

    public Authentication newAuthenticationTLS(String certFilePath, String keyFilePath) {
        return new org.apache.pulsar.client.impl.auth.AuthenticationTls(certFilePath, keyFilePath);
    }

    public Authentication createAuthentication(String authPluginClassName, String authParamsString) throws PulsarClientException.UnsupportedAuthenticationException {
        return org.apache.pulsar.client.impl.AuthenticationUtil.create(authPluginClassName, authParamsString);
    }

    public Authentication createAuthentication(String authPluginClassName, Map<String, String> authParams)
            throws PulsarClientException.UnsupportedAuthenticationException {
        return org.apache.pulsar.client.impl.AuthenticationUtil.create(authPluginClassName, authParams);
    }

    public Schema<byte[]> newBytesSchema() {
        return new org.apache.pulsar.client.impl.schema.BytesSchema();
    }

    public Schema<String> newStringSchema() {
        return new org.apache.pulsar.client.impl.schema.StringSchema();
    }

    public Schema<String> newStringSchema(Charset charset) {
        return new org.apache.pulsar.client.impl.schema.StringSchema(charset);
    }

    public Schema<Byte> newByteSchema() {
        return new org.apache.pulsar.client.impl.schema.ByteSchema();
    }

    public Schema<Short> newShortSchema() {
        return new org.apache.pulsar.client.impl.schema.ShortSchema();
    }

    public Schema<Integer> newIntSchema() {
        return new org.apache.pulsar.client.impl.schema.IntSchema();
    }

    public Schema<Long> newLongSchema() {
        return new org.apache.pulsar.client.impl.schema.LongSchema();
    }

    public Schema<Boolean> newBooleanSchema() {
        return new org.apache.pulsar.client.impl.schema.BooleanSchema();
    }

    public Schema<ByteBuffer> newByteBufferSchema() {
        return new org.apache.pulsar.client.impl.schema.ByteBufferSchema();
    }

    public Schema<Float> newFloatSchema() {
        return new org.apache.pulsar.client.impl.schema.FloatSchema();
    }

    public Schema<Double> newDoubleSchema() {
        return new org.apache.pulsar.client.impl.schema.DoubleSchema();
    }

    public Schema<Date> newDateSchema() {
        return org.apache.pulsar.client.impl.schema.DateSchema.of();
    }

    public Schema<Time> newTimeSchema() {
        return org.apache.pulsar.client.impl.schema.TimeSchema.of();
    }

    public Schema<Timestamp> newTimestampSchema() {
        return org.apache.pulsar.client.impl.schema.TimestampSchema.of();
    }

    public Schema<Instant> newInstantSchema() {
        return org.apache.pulsar.client.impl.schema.InstantSchema.of();
    }

    public Schema<LocalDate> newLocalDateSchema() {
        return org.apache.pulsar.client.impl.schema.LocalDateSchema.of();
    }

    public Schema<LocalTime> newLocalTimeSchema() {
        return org.apache.pulsar.client.impl.schema.LocalTimeSchema.of();
    }

    public Schema<LocalDateTime> newLocalDateTimeSchema() {
        return org.apache.pulsar.client.impl.schema.LocalDateTimeSchema.of();
    }

    public <T> Schema<T> newAvroSchema(SchemaDefinition schemaDefinition) {
        return org.apache.pulsar.client.impl.schema.AvroSchema.of(schemaDefinition);
    }

    public <T extends com.google.protobuf.GeneratedMessageV3> Schema<T> newProtobufSchema(
            SchemaDefinition schemaDefinition) {
        return org.apache.pulsar.client.impl.schema.ProtobufSchema.of(schemaDefinition);
    }

    public <T extends com.google.protobuf.GeneratedMessageV3> Schema<T> newProtobufNativeSchema(
            SchemaDefinition schemaDefinition) {
        return org.apache.pulsar.client.impl.schema.ProtobufNativeSchema.of(schemaDefinition);
    }

    public <T> Schema<T> newJSONSchema(SchemaDefinition schemaDefinition) {
        return org.apache.pulsar.client.impl.schema.JSONSchema.of(schemaDefinition);
    }

    public Schema<GenericRecord> newAutoConsumeSchema() {
        return new org.apache.pulsar.client.impl.schema.AutoConsumeSchema();
    }

    public Schema<byte[]> newAutoProduceSchema() {
        return new org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema();
    }

    public Schema<byte[]> newAutoProduceSchema(Schema<?> schema) {
        return new org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema(schema);
    }

    public Schema<byte[]> newAutoProduceValidatedAvroSchema(Object schema) {
        return new org.apache.pulsar.client.impl.schema.NativeAvroBytesSchema(schema);
    }

    public Schema<KeyValue<byte[], byte[]>> newKeyValueBytesSchema() {
        return org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl.kvBytes();
    }

    public <K, V> Schema<KeyValue<K, V>> newKeyValueSchema(Schema<K> keySchema, Schema<V> valueSchema) {
        return org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl.of(keySchema, valueSchema);
    }

    public <K, V> Schema<KeyValue<K, V>> newKeyValueSchema(Schema<K> keySchema, Schema<V> valueSchema,
                                                    KeyValueEncodingType keyValueEncodingType) {
        return org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl.of(keySchema, valueSchema, keyValueEncodingType);
    }

    public <K, V> Schema<KeyValue<K, V>> newKeyValueSchema(Class<K> key, Class<V> value, SchemaType type) {
        return org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl.of(key, value, type);
    }

    public Schema<?> getSchema(SchemaInfo schemaInfo) {
        return org.apache.pulsar.client.impl.schema.AutoConsumeSchema.getSchema(schemaInfo);
    }

    public GenericSchema<GenericRecord> getGenericSchema(SchemaInfo schemaInfo) {
        switch (schemaInfo.getType()) {
            case PROTOBUF_NATIVE:
                return org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema.of(schemaInfo);
            default:
                return org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl.of(schemaInfo);
        }

    }

    public RecordSchemaBuilder newRecordSchemaBuilder(String name) {
        return new org.apache.pulsar.client.impl.schema.RecordSchemaBuilderImpl(name);
    }

    /**
     * Decode the kv encoding type from the schema info.
     *
     * @param schemaInfo the schema info
     * @return the kv encoding type
     */
    public KeyValueEncodingType decodeKeyValueEncodingType(SchemaInfo schemaInfo) {
        return org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo.decodeKeyValueEncodingType(schemaInfo);
    }

    /**
     * Encode key & value into schema into a KeyValue schema.
     *
     * @param keySchema            the key schema
     * @param valueSchema          the value schema
     * @param keyValueEncodingType the encoding type to encode and decode key value pair
     * @return the final schema info
     */
    public <K, V> SchemaInfo encodeKeyValueSchemaInfo(Schema<K> keySchema,
                                               Schema<V> valueSchema,
                                               KeyValueEncodingType keyValueEncodingType) {
        return encodeKeyValueSchemaInfo("KeyValue", keySchema, valueSchema, keyValueEncodingType);
    }

    /**
     * Encode key & value into schema into a KeyValue schema.
     *
     * @param schemaName           the final schema name
     * @param keySchema            the key schema
     * @param valueSchema          the value schema
     * @param keyValueEncodingType the encoding type to encode and decode key value pair
     * @return the final schema info
     */
    public <K, V> SchemaInfo encodeKeyValueSchemaInfo(String schemaName,
                                               Schema<K> keySchema,
                                               Schema<V> valueSchema,
                                               KeyValueEncodingType keyValueEncodingType) {
        return org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo.encodeKeyValueSchemaInfo(schemaName, keySchema, valueSchema, keyValueEncodingType);
    }

    /**
     * Decode the key/value schema info to get key schema info and value schema info.
     *
     * @param schemaInfo key/value schema info.
     * @return the pair of key schema info and value schema info
     */
    public KeyValue<SchemaInfo, SchemaInfo> decodeKeyValueSchemaInfo(SchemaInfo schemaInfo) {
        return org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
    }

    /**
     * Jsonify the schema info.
     *
     * @param schemaInfo the schema info
     * @return the jsonified schema info
     */
    public String jsonifySchemaInfo(SchemaInfo schemaInfo) {
        return org.apache.pulsar.client.impl.schema.SchemaUtils.jsonifySchemaInfo(schemaInfo);
    }

    /**
     * Jsonify the schema info with version.
     *
     * @param schemaInfoWithVersion the schema info with version
     * @return the jsonified schema info with version
     */
    public String jsonifySchemaInfoWithVersion(SchemaInfoWithVersion schemaInfoWithVersion) {
        return org.apache.pulsar.client.impl.schema.SchemaUtils.jsonifySchemaInfoWithVersion(schemaInfoWithVersion);
    }

    /**
     * Jsonify the key/value schema info.
     *
     * @param kvSchemaInfo the key/value schema info
     * @return the jsonified schema info
     */
    public String jsonifyKeyValueSchemaInfo(KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo) {
        return org.apache.pulsar.client.impl.schema.SchemaUtils.jsonifyKeyValueSchemaInfo(kvSchemaInfo);
    }

    /**
     * Convert the key/value schema data.
     *
     * @param kvSchemaInfo the key/value schema info
     * @return the convert key/value schema data string
     */
    public String convertKeyValueSchemaInfoDataToString(KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo) throws IOException {
        return org.apache.pulsar.client.impl.schema.SchemaUtils.convertKeyValueSchemaInfoDataToString(kvSchemaInfo);
    }

    /**
     * Convert the key/value schema info data json bytes to key/value schema info data bytes.
     *
     * @param keyValueSchemaInfoDataJsonBytes the key/value schema info data json bytes
     * @return the key/value schema info data bytes
     */
    public byte[] convertKeyValueDataStringToSchemaInfoSchema(byte[] keyValueSchemaInfoDataJsonBytes) throws IOException {
        return org.apache.pulsar.client.impl.schema.SchemaUtils.convertKeyValueDataStringToSchemaInfoSchema(keyValueSchemaInfoDataJsonBytes);
    }

    public BatcherBuilder newDefaultBatcherBuilder() {
        return new org.apache.pulsar.client.impl.DefaultBatcherBuilder();
    }

    public BatcherBuilder newKeyBasedBatcherBuilder() {
        return new org.apache.pulsar.client.impl.KeyBasedBatcherBuilder();
    }

}
