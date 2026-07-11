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
package org.apache.pulsar.client.api.v5.internal;

import java.io.IOException;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.v5.Checkpoint;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.api.v5.schema.GenericRecord;
import org.apache.pulsar.client.api.v5.schema.Schema;

/**
 * Service Provider Interface for the Pulsar client implementation.
 *
 * <p>The implementation module registers itself via {@code META-INF/services}
 * using the standard Java {@link ServiceLoader} mechanism.
 *
 */
public interface PulsarClientProvider {

    // --- Client builder ---

    PulsarClientBuilder newClientBuilder();

    // --- MessageId ---

    MessageId messageIdFromBytes(byte[] data) throws IOException;

    MessageId earliestMessageId();

    MessageId latestMessageId();

    // --- Schemas ---

    Schema<byte[]> bytesSchema();

    Schema<String> stringSchema();

    Schema<Boolean> booleanSchema();

    Schema<Byte> byteSchema();

    Schema<Short> shortSchema();

    Schema<Integer> intSchema();

    Schema<Long> longSchema();

    Schema<Float> floatSchema();

    Schema<Double> doubleSchema();

    <T> Schema<T> jsonSchema(Class<T> pojo);

    <T> Schema<T> avroSchema(Class<T> pojo);

    <T extends com.google.protobuf.Message> Schema<T> protobufSchema(Class<T> clazz);

    Schema<byte[]> autoProduceBytesSchema();

    Schema<?> genericSchema(org.apache.pulsar.client.api.v5.schema.SchemaInfo schemaInfo);

    Schema<byte[]> autoProduceBytesSchema(Schema<?> base);

    Schema<GenericRecord> autoConsumeSchema();

    // --- Checkpoint ---

    Checkpoint checkpointFromBytes(byte[] data) throws IOException;

    Checkpoint earliestCheckpoint();

    Checkpoint latestCheckpoint();

    // --- Authentication ---

    Authentication authenticationToken(String token);

    Authentication authenticationToken(Supplier<String> tokenSupplier);

    Authentication authenticationTls(String certFilePath, String keyFilePath);

    Authentication createAuthentication(String className, String params) throws PulsarClientException;

    Authentication createAuthentication(String className, Map<String, String> params) throws PulsarClientException;

    // --- Singleton access ---

    static PulsarClientProvider get() {
        return Holder.INSTANCE;
    }

    /**
     * Lazy initialization holder for the provider singleton.
     */
    final class Holder {
        static final PulsarClientProvider INSTANCE = ServiceLoader.load(PulsarClientProvider.class)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No PulsarClientProvider found on the classpath. "
                                + "Add pulsar-client to your dependencies."));

        private Holder() {
        }
    }
}
