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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

public class PulsarKafkaSchema<T> implements Schema<T> {

    private Serializer<T> kafkaSerializer;

    private Deserializer<T> kafkaDeserializer;

    private String topic;

    public void setKafkaSerializer(Serializer<T> serializer) {
        this.kafkaSerializer = serializer;
    }

    public Serializer<T> getKafkaSerializer() {
        return kafkaSerializer;
    }

    public void setKafkaDeserializer(Deserializer<T> deserializer) {
        this.kafkaDeserializer = deserializer;
    }

    public Deserializer<T> getKafkaDeserializer() {
        return kafkaDeserializer;
    }

    public void initSerialize(ProducerConfig producerConfig, String classConfig, boolean isKey) {
        this.kafkaSerializer = producerConfig.getConfiguredInstance(classConfig, Serializer.class);
        this.kafkaSerializer.configure(producerConfig.originals(), isKey);
    }

    public void initDeserialize(ConsumerConfig consumerConfig, String classConfig, boolean isKey) {
        this.kafkaDeserializer = consumerConfig.getConfiguredInstance(classConfig, Deserializer.class);
        this.kafkaDeserializer.configure(consumerConfig.originals(), isKey);
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public byte[] encode(T message) {
        return kafkaSerializer.serialize(this.topic, message);
    }

    @Override
    public T decode(byte[] message) {
        return kafkaDeserializer.deserialize(this.topic, message);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return Schema.BYTES.getSchemaInfo();
    }
}
