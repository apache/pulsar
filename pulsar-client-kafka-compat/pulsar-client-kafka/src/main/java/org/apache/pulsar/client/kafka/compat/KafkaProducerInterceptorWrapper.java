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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerInterceptor;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

/**
 * A wrapper for Kafka's {@link org.apache.kafka.clients.producer.ProducerInterceptor} to make pulsar support
 * Kafka ProducerInterceptor. It holds an instance of {@link org.apache.kafka.clients.producer.ProducerInterceptor}
 * and it'll delegate all invocation to that instance.
 * <p>
 * Extend {@link ProducerInterceptor<byte[]>} as all Pulsar {@link Message} created by
 * {@link org.apache.kafka.clients.producer.PulsarKafkaProducer} is of type byte[].
 *
 */
public class KafkaProducerInterceptorWrapper<K, V> implements ProducerInterceptor<byte[]> {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerInterceptorWrapper.class);

    final private org.apache.kafka.clients.producer.ProducerInterceptor<K, V> kafkaProducerInterceptor;

    // For serializer key/value, and to determine the deserializer for key/value.
    private final Schema<K> keySchema;

    private final Schema<V> valueSchema;

    // Keep the topic, as each Pulsar producer will tie to a Kafka topic, and ProducerInterceptor will tie to a Pulsar
    // producer, it's safe to set it as final.
    private final String topic;

    private Schema<byte[]> scheme;

    private long eventTime;

    private String partitionID;

    /**
     * Create a wrapper of type {@link ProducerInterceptor} that will delegate all work to underlying Kafka's interceptor.
     *
     * @param kafkaProducerInterceptor Underlying instance of {@link org.apache.kafka.clients.producer.ProducerInterceptor<K, V>}
     *                                 that this wrapper will delegate work to.
     * @param keySerializer            {@link Serializer} used to serialize Kafka {@link ProducerRecord#key}.
     * @param valueSerializer          {@link Serializer} used to serialize Kafka {@link ProducerRecord#value}.
     * @param topic                    Topic this {@link ProducerInterceptor} will be associated to.
     */
    public KafkaProducerInterceptorWrapper(org.apache.kafka.clients.producer.ProducerInterceptor<K, V> kafkaProducerInterceptor,
                                           Schema<K> keySchema,
                                           Schema<V> valueSchema,
                                           String topic) {
        this.kafkaProducerInterceptor = kafkaProducerInterceptor;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.topic = topic;
    }

    /**
     * Called when interceptor is closed.
     * The wrapper itself doesn't own any resource, just call underlying {@link org.apache.kafka.clients.producer.ProducerInterceptor#close()}
     */
    @Override
    public void close() {
        kafkaProducerInterceptor.close();
    }

    /**
     * It tries to convert a Pulsar {@link Message} to a Kafka{@link ProducerRecord}, pass it to underlying
     * {@link org.apache.kafka.clients.producer.ProducerInterceptor#onSend(ProducerRecord)} then convert the output
     * back to Pulsar {@link Message}.
     * <p>
     * When build a Pulsar {@link Message} at {@link org.apache.kafka.clients.producer.PulsarKafkaProducer#buildMessage}
     * schema, eventtime, partitionID, key and value are set. All this information will be preserved during the conversion.
     *
     * @param producer the producer which contains the interceptor, will be ignored as Kafka
     *                 {@link org.apache.kafka.clients.producer.ProducerInterceptor} doesn't use it.
     * @param message message to send
     * @return Processed message.
     */
    @Override
    public Message<byte[]> beforeSend(Producer<byte[]> producer, Message<byte[]> message) {
        return toPulsarMessage(kafkaProducerInterceptor.onSend(toKafkaRecord(message)));
    }

    /**
     * Delegate work to {@link org.apache.kafka.clients.producer.ProducerInterceptor#onAcknowledgement}
     * @param producer the producer which contains the interceptor.
     * @param message the message that application sends
     * @param msgId the message id that assigned by the broker; null if send failed.
     * @param exception the exception on sending messages, null indicates send has succeed.
     */
    @Override
    public void onSendAcknowledgement(Producer<byte[]> producer, Message<byte[]> message, MessageId msgId, Throwable exception) {
        try {
            PulsarApi.MessageMetadata.Builder messageMetadataBuilder = ((MessageImpl<byte[]>)message).getMessageBuilder();
            partitionID = getPartitionID(messageMetadataBuilder);
            TopicPartition topicPartition = new TopicPartition(topic, Integer.parseInt(partitionID));
            kafkaProducerInterceptor.onAcknowledgement(new RecordMetadata(topicPartition,
                                                                -1L,
                                                                -1L,
                                                                messageMetadataBuilder.getEventTime(),
                                                                -1L,
                                                                message.getKeyBytes().length,
                                                                message.getValue().length), new Exception(exception));
        } catch (NumberFormatException e) {
            String errorMessage = "Unable to convert partitionID to integer: " + e.getMessage();
            log.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
    }

    /**
     * Convert a Kafka {@link ProducerRecord} to a Pulsar {@link Message}.
     *
     * @param producerRecord Kafka record to be convert.
     * @return Pulsar message.
     */
    private Message<byte[]> toPulsarMessage(ProducerRecord<K, V> producerRecord) {
        TypedMessageBuilderImpl typedMessageBuilder = new TypedMessageBuilderImpl(null, scheme);
        typedMessageBuilder.key(serializeKey(topic, producerRecord.key()));
        if (valueSchema instanceof PulsarKafkaSchema) {
            ((PulsarKafkaSchema<V>) valueSchema).setTopic(topic);
        }
        typedMessageBuilder.value(valueSchema.encode(producerRecord.value()));
        typedMessageBuilder.eventTime(eventTime);
        typedMessageBuilder.property(KafkaMessageRouter.PARTITION_ID, partitionID);
        return typedMessageBuilder.getMessage();
    }

    /**
     * Convert a Pulsar {@link Message} to a Kafka {@link ProducerRecord}.
     * First it'll store those field that Kafka record doesn't need such as schema.
     * Then it try to deserialize the value as it's been serialized to byte[] when creating the message.
     *
     * @param message Pulsar message to be convert.
     * @return Kafka record.
     */
    private ProducerRecord<K, V> toKafkaRecord(Message<byte[]> message) {
        V value;
        if (valueSchema instanceof PulsarKafkaSchema) {
            PulsarKafkaSchema<V> pulsarKeyKafkaSchema = (PulsarKafkaSchema<V>) valueSchema;
            Deserializer valueDeserializer = getDeserializer((pulsarKeyKafkaSchema.getKafkaSerializer()));
            value = (V) valueDeserializer.deserialize(topic, message.getValue());
        } else {
            value = valueSchema.decode(message.getValue());
        }
        try {
            scheme = (Schema<byte[]>) FieldUtils.readField(message, "schema", true);
            PulsarApi.MessageMetadata.Builder messageMetadataBuilder = ((MessageImpl<byte[]>)message).getMessageBuilder();
            partitionID = getPartitionID(messageMetadataBuilder);
            eventTime = message.getEventTime();
            return new ProducerRecord<>(topic, Integer.parseInt(partitionID), eventTime, deserializeKey(topic, message.getKey()), value);
        } catch (NumberFormatException e) {
            // If not able to parse partitionID, ignore it.
            return new ProducerRecord<>(topic, deserializeKey(topic, message.getKey()), value);
        } catch (IllegalAccessException e) {
            String errorMessage = "Unable to get the schema of message due to " + e.getMessage();
            log.error(errorMessage);
            throw new RuntimeException(errorMessage);
        }
    }

    private String serializeKey(String topic, K key) {
        // If key is a String, we can use it as it is, otherwise, serialize to byte[] and encode in base64
        if (key instanceof String) {
            return (String) key;
        }
        if (keySchema instanceof PulsarKafkaSchema) {
            ((PulsarKafkaSchema<K>) keySchema).setTopic(topic);
        }
        byte[] keyBytes = keySchema.encode(key);
        return Base64.getEncoder().encodeToString(keyBytes);
    }

    private K deserializeKey(String topic, String key) {
        if (keySchema instanceof PulsarKafkaSchema) {
            PulsarKafkaSchema<K> pulsarKeyKafkaSchema = (PulsarKafkaSchema<K>) keySchema;
            // If key is a String, we can use it as it is, otherwise, serialize to byte[] and encode in base64
            if (pulsarKeyKafkaSchema.getKafkaSerializer() instanceof StringSerializer) {
                return (K) key;
            }

            Deserializer keyDeserializer = getDeserializer(pulsarKeyKafkaSchema.getKafkaSerializer());
            return (K) keyDeserializer.deserialize(topic, Base64.getDecoder().decode(key));
        }
        return keySchema.decode(Base64.getDecoder().decode(key));
    }

    /**
     * Try to get the partitionID from messageMetadataBuilder.
     * As it is set in {@link org.apache.kafka.clients.producer.PulsarKafkaProducer#buildMessage}, can guarantee
     * a partitionID will be return.
     *
     * @param messageMetadataBuilder
     * @return PartitionID
     */
    private String getPartitionID(PulsarApi.MessageMetadata.Builder messageMetadataBuilder) {
        return messageMetadataBuilder.getPropertiesList()
                                    .stream()
                                    .filter(keyValue -> keyValue.getKey().equals(KafkaMessageRouter.PARTITION_ID))
                                    .findFirst()
                                    .get()
                                    .getValue();
    }

    static Deserializer getDeserializer(Serializer serializer) {
        if (serializer instanceof StringSerializer) {
            return new StringDeserializer();
        } else if (serializer instanceof LongSerializer) {
            return new LongDeserializer();
        } else if (serializer instanceof IntegerSerializer) {
            return new IntegerDeserializer();
        } else if (serializer instanceof DoubleSerializer) {
            return new DoubleDeserializer();
        } else if (serializer instanceof BytesSerializer) {
            return new BytesDeserializer();
        } else if (serializer instanceof ByteBufferSerializer) {
            return new ByteBufferDeserializer();
        } else if (serializer instanceof ByteArraySerializer) {
            return new ByteArrayDeserializer();
        } else {
            throw new IllegalArgumentException(serializer.getClass().getName() + " is not a valid or supported subclass of org.apache.kafka.common.serialization.Serializer.");
        }
    }
}
