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
package org.apache.pulsar.client.kafka.compat;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

import static com.google.common.base.Preconditions.checkArgument;

public class PulsarKafkaSchema<T> implements Schema<T> {

    private final Serializer<T> kafkaSerializer;

    private final Deserializer<T> kafkaDeserializer;

    private String topic;

    public PulsarKafkaSchema(Serializer<T> serializer) {
        this(serializer, null);
    }

    public PulsarKafkaSchema(Deserializer<T> deserializer) {
        this(null, deserializer);
    }

    public PulsarKafkaSchema(Serializer<T> serializer, Deserializer<T> deserializer) {
        this.kafkaSerializer = serializer;
        this.kafkaDeserializer = deserializer;
    }

    public Serializer<T> getKafkaSerializer() {
        return kafkaSerializer;
    }

    public Deserializer<T> getKafkaDeserializer() {
        return kafkaDeserializer;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public byte[] encode(T message) {
        checkArgument(kafkaSerializer != null, "Kafka serializer is not initialized yet");
        return kafkaSerializer.serialize(this.topic, message);
    }

    @Override
    public T decode(byte[] message) {
        checkArgument(kafkaDeserializer != null, "Kafka deserializer is not initialized yet");
        return kafkaDeserializer.deserialize(this.topic, message);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return Schema.BYTES.getSchemaInfo();
    }

    @Override
    public Schema<T> clone() {
        return this;
    }
}
