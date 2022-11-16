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
import org.apache.pulsar.client.api.MessagePayloadFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaDefinitionBuilder;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.BooleanSchema;
import org.apache.pulsar.client.impl.schema.ByteBufferSchema;
import org.apache.pulsar.client.impl.schema.ByteSchema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.client.impl.schema.DateSchema;
import org.apache.pulsar.client.impl.schema.DoubleSchema;
import org.apache.pulsar.client.impl.schema.FloatSchema;
import org.apache.pulsar.client.impl.schema.InstantSchema;
import org.apache.pulsar.client.impl.schema.IntSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.client.impl.schema.LocalDateSchema;
import org.apache.pulsar.client.impl.schema.LocalDateTimeSchema;
import org.apache.pulsar.client.impl.schema.LocalTimeSchema;
import org.apache.pulsar.client.impl.schema.LongSchema;
import org.apache.pulsar.client.impl.schema.NativeAvroBytesSchema;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.apache.pulsar.client.impl.schema.RecordSchemaBuilderImpl;
import org.apache.pulsar.client.impl.schema.SchemaDefinitionBuilderImpl;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.client.impl.schema.SchemaUtils;
import org.apache.pulsar.client.impl.schema.ShortSchema;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.client.impl.schema.TimeSchema;
import org.apache.pulsar.client.impl.schema.TimestampSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericProtobufNativeSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
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
public final class PulsarClientImplementationBindingImpl implements PulsarClientImplementationBinding {

    public <T> SchemaDefinitionBuilder<T> newSchemaDefinitionBuilder() {
        return new SchemaDefinitionBuilderImpl();
    }

    public ClientBuilder newClientBuilder() {
        return new ClientBuilderImpl();
    }

    public MessageId newMessageId(long ledgerId, long entryId, int partitionIndex) {
        return new MessageIdImpl(ledgerId, entryId, partitionIndex);
    }

    public MessageId newMessageIdFromByteArray(byte[] data) throws IOException {
        return MessageIdImpl.fromByteArray(data);
    }

    public MessageId newMessageIdFromByteArrayWithTopic(byte[] data, String topicName) throws IOException {
        return MessageIdImpl.fromByteArrayWithTopic(data, topicName);
    }

    public Authentication newAuthenticationToken(String token) {
        return new AuthenticationToken(token);
    }

    public Authentication newAuthenticationToken(Supplier<String> supplier) {
        return new AuthenticationToken(supplier);
    }

    public Authentication newAuthenticationTLS(String certFilePath, String keyFilePath) {
        return new AuthenticationTls(certFilePath, keyFilePath);
    }

    public Authentication createAuthentication(String authPluginClassName, String authParamsString)
            throws PulsarClientException.UnsupportedAuthenticationException {
        return AuthenticationUtil.create(authPluginClassName, authParamsString);
    }

    public Authentication createAuthentication(String authPluginClassName, Map<String, String> authParams)
            throws PulsarClientException.UnsupportedAuthenticationException {
        return AuthenticationUtil.create(authPluginClassName, authParams);
    }

    public Schema<byte[]> newBytesSchema() {
        return new BytesSchema();
    }

    public Schema<String> newStringSchema() {
        return new StringSchema();
    }

    public Schema<String> newStringSchema(Charset charset) {
        return new StringSchema(charset);
    }

    public Schema<Byte> newByteSchema() {
        return new ByteSchema();
    }

    public Schema<Short> newShortSchema() {
        return new ShortSchema();
    }

    public Schema<Integer> newIntSchema() {
        return new IntSchema();
    }

    public Schema<Long> newLongSchema() {
        return new LongSchema();
    }

    public Schema<Boolean> newBooleanSchema() {
        return new BooleanSchema();
    }

    public Schema<ByteBuffer> newByteBufferSchema() {
        return new ByteBufferSchema();
    }

    public Schema<Float> newFloatSchema() {
        return new FloatSchema();
    }

    public Schema<Double> newDoubleSchema() {
        return new DoubleSchema();
    }

    public Schema<Date> newDateSchema() {
        return DateSchema.of();
    }

    public Schema<Time> newTimeSchema() {
        return TimeSchema.of();
    }

    public Schema<Timestamp> newTimestampSchema() {
        return TimestampSchema.of();
    }

    public Schema<Instant> newInstantSchema() {
        return InstantSchema.of();
    }

    public Schema<LocalDate> newLocalDateSchema() {
        return LocalDateSchema.of();
    }

    public Schema<LocalTime> newLocalTimeSchema() {
        return LocalTimeSchema.of();
    }

    public Schema<LocalDateTime> newLocalDateTimeSchema() {
        return LocalDateTimeSchema.of();
    }

    public <T> Schema<T> newAvroSchema(SchemaDefinition schemaDefinition) {
        return AvroSchema.of(schemaDefinition);
    }

    public <T extends com.google.protobuf.GeneratedMessageV3> Schema<T> newProtobufSchema(
            SchemaDefinition schemaDefinition) {
        return ProtobufSchema.of(schemaDefinition);
    }

    public <T extends com.google.protobuf.GeneratedMessageV3> Schema<T> newProtobufNativeSchema(
            SchemaDefinition schemaDefinition) {
        return ProtobufNativeSchema.of(schemaDefinition);
    }

    public <T> Schema<T> newJSONSchema(SchemaDefinition schemaDefinition) {
        return JSONSchema.of(schemaDefinition);
    }

    public Schema<GenericRecord> newAutoConsumeSchema() {
        return new AutoConsumeSchema();
    }

    public Schema<byte[]> newAutoProduceSchema() {
        return new AutoProduceBytesSchema();
    }

    public Schema<byte[]> newAutoProduceSchema(Schema<?> schema) {
        return new AutoProduceBytesSchema(schema);
    }

    public Schema<byte[]> newAutoProduceValidatedAvroSchema(Object schema) {
        return new NativeAvroBytesSchema(schema);
    }

    public Schema<KeyValue<byte[], byte[]>> newKeyValueBytesSchema() {
        return KeyValueSchemaImpl.kvBytes();
    }

    public <K, V> Schema<KeyValue<K, V>> newKeyValueSchema(Schema<K> keySchema, Schema<V> valueSchema,
                                                    KeyValueEncodingType keyValueEncodingType) {
        return KeyValueSchemaImpl.of(keySchema, valueSchema, keyValueEncodingType);
    }

    public <K, V> Schema<KeyValue<K, V>> newKeyValueSchema(Class<K> key, Class<V> value, SchemaType type) {
        return KeyValueSchemaImpl.of(key, value, type);
    }

    public Schema<?> getSchema(SchemaInfo schemaInfo) {
        return AutoConsumeSchema.getSchema(schemaInfo);
    }

    public GenericSchema<GenericRecord> getGenericSchema(SchemaInfo schemaInfo) {
        switch (schemaInfo.getType()) {
            case PROTOBUF_NATIVE:
                return GenericProtobufNativeSchema.of(schemaInfo);
            default:
                return GenericSchemaImpl.of(schemaInfo);
        }

    }

    public RecordSchemaBuilder newRecordSchemaBuilder(String name) {
        return new RecordSchemaBuilderImpl(name);
    }

    /**
     * Decode the kv encoding type from the schema info.
     *
     * @param schemaInfo the schema info
     * @return the kv encoding type
     */
    public KeyValueEncodingType decodeKeyValueEncodingType(SchemaInfo schemaInfo) {
        return KeyValueSchemaInfo.decodeKeyValueEncodingType(schemaInfo);
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
        return KeyValueSchemaInfo.encodeKeyValueSchemaInfo(schemaName, keySchema, valueSchema, keyValueEncodingType);
    }

    /**
     * Decode the key/value schema info to get key schema info and value schema info.
     *
     * @param schemaInfo key/value schema info.
     * @return the pair of key schema info and value schema info
     */
    public KeyValue<SchemaInfo, SchemaInfo> decodeKeyValueSchemaInfo(SchemaInfo schemaInfo) {
        return KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
    }

    /**
     * Jsonify the schema info.
     *
     * @param schemaInfo the schema info
     * @return the jsonified schema info
     */
    public String jsonifySchemaInfo(SchemaInfo schemaInfo) {
        return SchemaUtils.jsonifySchemaInfo(schemaInfo);
    }

    /**
     * Jsonify the schema info with version.
     *
     * @param schemaInfoWithVersion the schema info with version
     * @return the jsonified schema info with version
     */
    public String jsonifySchemaInfoWithVersion(SchemaInfoWithVersion schemaInfoWithVersion) {
        return SchemaUtils.jsonifySchemaInfoWithVersion(schemaInfoWithVersion);
    }

    /**
     * Jsonify the key/value schema info.
     *
     * @param kvSchemaInfo the key/value schema info
     * @return the jsonified schema info
     */
    public String jsonifyKeyValueSchemaInfo(KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo) {
        return SchemaUtils.jsonifyKeyValueSchemaInfo(kvSchemaInfo);
    }

    /**
     * Convert the key/value schema data.
     *
     * @param kvSchemaInfo the key/value schema info
     * @return the convert key/value schema data string
     */
    public String convertKeyValueSchemaInfoDataToString(KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo)
            throws IOException {
        return SchemaUtils.convertKeyValueSchemaInfoDataToString(kvSchemaInfo);
    }

    /**
     * Convert the key/value schema info data json bytes to key/value schema info data bytes.
     *
     * @param keyValueSchemaInfoDataJsonBytes the key/value schema info data json bytes
     * @return the key/value schema info data bytes
     */
    public byte[] convertKeyValueDataStringToSchemaInfoSchema(byte[] keyValueSchemaInfoDataJsonBytes)
            throws IOException {
        return SchemaUtils.convertKeyValueDataStringToSchemaInfoSchema(keyValueSchemaInfoDataJsonBytes);
    }

    public BatcherBuilder newDefaultBatcherBuilder() {
        return new DefaultBatcherBuilder();
    }

    public BatcherBuilder newKeyBasedBatcherBuilder() {
        return new KeyBasedBatcherBuilder();
    }

    public MessagePayloadFactory newDefaultMessagePayloadFactory() {
        return new MessagePayloadFactoryImpl();
    }

    public SchemaInfo newSchemaInfoImpl(String name, byte[] schema, SchemaType type, long timestamp,
                                        Map<String, String> propertiesValue) {
        return new SchemaInfoImpl(name, schema, type, timestamp, propertiesValue);
    }
}
