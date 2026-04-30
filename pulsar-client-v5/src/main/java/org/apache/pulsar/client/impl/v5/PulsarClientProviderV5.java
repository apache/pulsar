/*
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
package org.apache.pulsar.client.impl.v5;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.internal.PulsarClientProvider;
import org.apache.pulsar.client.api.v5.schema.Schema;

/**
 * ServiceLoader-registered implementation of PulsarClientProvider.
 * Bridges v5 API types to v4 implementation classes.
 */
public final class PulsarClientProviderV5 implements PulsarClientProvider {

    @Override
    public PulsarClientBuilder newClientBuilder() {
        return new PulsarClientBuilderV5();
    }

    // --- MessageId ---

    @Override
    public MessageId messageIdFromBytes(byte[] data) throws IOException {
        return MessageIdV5.fromByteArray(data);
    }

    @Override
    public MessageId earliestMessageId() {
        return MessageIdV5.EARLIEST;
    }

    @Override
    public MessageId latestMessageId() {
        return MessageIdV5.LATEST;
    }

    // --- Schemas ---

    @Override
    public Schema<byte[]> bytesSchema() {
        return SchemaAdapter.toV5(org.apache.pulsar.client.api.Schema.BYTES);
    }

    @Override
    public Schema<String> stringSchema() {
        return SchemaAdapter.toV5(org.apache.pulsar.client.api.Schema.STRING);
    }

    @Override
    public Schema<Boolean> booleanSchema() {
        return SchemaAdapter.toV5(org.apache.pulsar.client.api.Schema.BOOL);
    }

    @Override
    public Schema<Byte> byteSchema() {
        return SchemaAdapter.toV5(org.apache.pulsar.client.api.Schema.INT8);
    }

    @Override
    public Schema<Short> shortSchema() {
        return SchemaAdapter.toV5(org.apache.pulsar.client.api.Schema.INT16);
    }

    @Override
    public Schema<Integer> intSchema() {
        return SchemaAdapter.toV5(org.apache.pulsar.client.api.Schema.INT32);
    }

    @Override
    public Schema<Long> longSchema() {
        return SchemaAdapter.toV5(org.apache.pulsar.client.api.Schema.INT64);
    }

    @Override
    public Schema<Float> floatSchema() {
        return SchemaAdapter.toV5(org.apache.pulsar.client.api.Schema.FLOAT);
    }

    @Override
    public Schema<Double> doubleSchema() {
        return SchemaAdapter.toV5(org.apache.pulsar.client.api.Schema.DOUBLE);
    }

    @Override
    public <T> Schema<T> jsonSchema(Class<T> pojo) {
        return SchemaAdapter.toV5(org.apache.pulsar.client.api.Schema.JSON(pojo));
    }

    @Override
    public <T> Schema<T> avroSchema(Class<T> pojo) {
        return SchemaAdapter.toV5(org.apache.pulsar.client.api.Schema.AVRO(pojo));
    }

    @Override
    public <T extends com.google.protobuf.Message> Schema<T> protobufSchema(Class<T> clazz) {
        return SchemaAdapter.toV5(org.apache.pulsar.client.api.Schema.PROTOBUF(clazz));
    }

    @Override
    public Schema<byte[]> autoProduceBytesSchema() {
        return SchemaAdapter.toV5(org.apache.pulsar.client.api.Schema.AUTO_PRODUCE_BYTES());
    }

    // --- Checkpoint ---

    @Override
    public Checkpoint checkpointFromBytes(byte[] data) throws IOException {
        return CheckpointV5.fromByteArray(data);
    }

    @Override
    public Checkpoint earliestCheckpoint() {
        return CheckpointV5.EARLIEST;
    }

    @Override
    public Checkpoint latestCheckpoint() {
        return CheckpointV5.LATEST;
    }

    @Override
    public Checkpoint checkpointAtTimestamp(Instant timestamp) {
        return CheckpointV5.atTimestamp(timestamp);
    }

    // --- Authentication ---

    @Override
    public Authentication authenticationToken(String token) {
        return AuthenticationAdapter.token(token);
    }

    @Override
    public Authentication authenticationToken(Supplier<String> tokenSupplier) {
        return AuthenticationAdapter.token(tokenSupplier);
    }

    @Override
    public Authentication authenticationTls(String certFilePath, String keyFilePath) {
        return AuthenticationAdapter.tls(certFilePath, keyFilePath);
    }

    @Override
    public Authentication createAuthentication(String className, String params)
            throws PulsarClientException {
        return AuthenticationAdapter.create(className, params);
    }

    @Override
    public Authentication createAuthentication(String className, Map<String, String> params)
            throws PulsarClientException {
        return AuthenticationAdapter.create(className, params);
    }
}
