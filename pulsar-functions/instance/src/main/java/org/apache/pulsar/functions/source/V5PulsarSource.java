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
package org.apache.pulsar.functions.source;

import static com.google.common.base.Preconditions.checkArgument;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.v5.Message;
import org.apache.pulsar.client.api.v5.MessageId;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.QueueConsumer;
import org.apache.pulsar.client.api.v5.QueueConsumerBuilder;
import org.apache.pulsar.client.api.v5.StreamConsumer;
import org.apache.pulsar.client.api.v5.config.BackoffPolicy;
import org.apache.pulsar.client.api.v5.config.DeadLetterPolicy;
import org.apache.pulsar.client.api.v5.config.ProcessingTimeoutPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.v5.V5Interop;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;

/**
 * Reads a component's input topics with the V5 client.
 *
 * <p>Each input topic gets its own V5 consumer, because a V5 consumer reads a single topic. A Shared subscription
 * uses a {@link QueueConsumer}: records are acknowledged individually, a failed record is negatively acknowledged,
 * and the processing timeout, negative-ack delay and dead letter policy apply. Any other subscription type
 * (Failover, Key_Shared, or EFFECTIVELY_ONCE processing) uses a {@link StreamConsumer}, which delivers in order and
 * splits the topic's key ranges across the component's instances. A stream only acknowledges cumulatively, so
 * completions go through a {@link StreamAckTracker}; it has no negative acknowledgment, so a failed record fails the
 * instance through its fatal handler, and the messages are delivered again from the last acknowledged position when
 * it restarts.
 *
 * <p>The records are {@link PulsarRecord}s over the v4 message that the V5 message wraps, so connectors that read the
 * schema version, the encryption context or the message itself keep working. The record's topic is the input
 * topic, not the segment the message came from.
 */
@CustomLog
public class V5PulsarSource<T> extends PushPulsarSource<T> {

    private static final Duration RECEIVE_TIMEOUT = Duration.ofMillis(100);
    private static final Duration RECEIVE_RETRY_DELAY = Duration.ofSeconds(1);
    private static final long CLOSE_TIMEOUT_MS = 10_000;

    private final MultiConsumerPulsarSourceConfig pulsarSourceConfig;
    private final Supplier<org.apache.pulsar.client.api.v5.PulsarClient> clientV5;
    private final String consumerName;
    private final List<Input> inputs = new ArrayList<>();
    private volatile boolean closed;
    private SourceContext sourceContext;

    /** One input topic with its consumer and receive thread. */
    private abstract class Input {
        final String topic;
        Thread receiveThread;

        Input(String topic) {
            this.topic = topic;
        }

        abstract Message<T> receive() throws PulsarClientException;

        abstract Record<T> buildRecord(Message<T> message);

        abstract void close() throws PulsarClientException;

        void start() {
            receiveThread = new Thread(this::receiveLoop, "v5-source-" + topic);
            receiveThread.setDaemon(true);
            receiveThread.start();
        }

        private void receiveLoop() {
            while (!closed) {
                try {
                    Message<T> message = receive();
                    if (message != null) {
                        consume(buildRecord(message));
                    }
                } catch (PulsarClientException.AlreadyClosedException e) {
                    return;
                } catch (Exception e) {
                    if (closed || Thread.currentThread().isInterrupted()) {
                        return;
                    }
                    log.warn().attr("topic", topic).exception(e).log("Failed to receive from the input topic");
                    try {
                        Thread.sleep(RECEIVE_RETRY_DELAY.toMillis());
                    } catch (InterruptedException ie) {
                        return;
                    }
                }
            }
        }

        PulsarRecord.PulsarRecordBuilder<T> recordBuilder(Message<T> message) {
            org.apache.pulsar.client.api.Message<T> v4Message = V5Interop.v4Message(message).orElseThrow(
                    () -> new IllegalStateException("Unexpected V5 message implementation " + message.getClass()));
            return PulsarRecord.<T>builder()
                    .message(v4Message)
                    .schema(recordSchema(v4Message))
                    .topicName(topic);
        }
    }

    private final class QueueInput extends Input {
        private final QueueConsumer<T> consumer;

        QueueInput(String topic, QueueConsumer<T> consumer) {
            super(topic);
            this.consumer = consumer;
        }

        @Override
        Message<T> receive() throws PulsarClientException {
            return consumer.receive(RECEIVE_TIMEOUT);
        }

        @Override
        Record<T> buildRecord(Message<T> message) {
            MessageId messageId = message.id();
            return recordBuilder(message)
                    .ackFunction(() -> consumer.acknowledge(messageId))
                    // a queue has no cumulative acknowledgment; each record is acknowledged on its own
                    .customAckFunction(cumulative -> consumer.acknowledge(messageId))
                    .failFunction(() -> consumer.negativeAcknowledge(messageId))
                    .build();
        }

        @Override
        void close() throws PulsarClientException {
            consumer.close();
        }
    }

    private final class StreamInput extends Input {
        private final StreamConsumer<T> consumer;
        private final StreamAckTracker<MessageId> ackTracker;

        StreamInput(String topic, StreamConsumer<T> consumer) {
            super(topic);
            this.consumer = consumer;
            this.ackTracker = new StreamAckTracker<>(consumer::acknowledgeCumulative);
        }

        @Override
        Message<T> receive() throws PulsarClientException {
            return consumer.receive(RECEIVE_TIMEOUT);
        }

        @Override
        Record<T> buildRecord(Message<T> message) {
            // tracked on the receive thread, so in receive order
            StreamAckTracker.Entry<MessageId> entry = ackTracker.track(message.id());
            return recordBuilder(message)
                    .ackFunction(() -> ackTracker.complete(entry))
                    .customAckFunction(cumulative -> {
                        if (cumulative) {
                            ackTracker.completeThrough(entry);
                        } else {
                            ackTracker.complete(entry);
                        }
                    })
                    .failFunction(() -> {
                        RuntimeException failure = new RuntimeException("Failed to process message " + message.id()
                                + " from " + topic + ": a stream subscription cannot negatively acknowledge, so the"
                                + " instance restarts from the last acknowledged position");
                        // fail() may be called from any thread, such as a producer callback or a connector's own
                        // thread, where a thrown exception would be lost and the acknowledgments would stall
                        sourceContext.fatal(failure);
                        throw failure;
                    })
                    .build();
        }

        @Override
        void close() throws PulsarClientException {
            consumer.close();
        }
    }

    /**
     * @param pulsarClient the v4 client, used to look up the input topics' schemas
     * @param clientV5 the V5 client that reads the input topics
     * @param consumerName the consumer name; it identifies the instance in a stream consumer group, so it must be
     *                     stable across restarts and unique among the component's instances
     */
    public V5PulsarSource(PulsarClient pulsarClient,
                          Supplier<org.apache.pulsar.client.api.v5.PulsarClient> clientV5,
                          MultiConsumerPulsarSourceConfig pulsarSourceConfig,
                          Map<String, String> properties,
                          ClassLoader functionClassLoader,
                          String consumerName) {
        super(pulsarClient, pulsarSourceConfig, properties, functionClassLoader);
        this.pulsarSourceConfig = pulsarSourceConfig;
        this.clientV5 = clientV5;
        this.consumerName = consumerName;
    }

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        log.info().attr("config", pulsarSourceConfig).log("Opening pulsar source with the V5 client");
        this.sourceContext = sourceContext;
        if (Boolean.TRUE.equals(pulsarSourceConfig.getSkipToLatest())) {
            throw new UnsupportedOperationException("skipToLatest is not supported with the V5 client");
        }
        Class<?> typeArg = Reflections.loadClass(pulsarSourceConfig.getTypeClassName(), functionClassLoader);
        checkArgument(!Void.class.equals(typeArg), "Input type of Pulsar Function cannot be Void");

        try {
            for (var e : pulsarSourceConfig.getTopicSchema().entrySet()) {
                String topic = e.getKey();
                PulsarSourceConsumerConfig<T> conf = buildPulsarSourceConsumerConfig(topic, e.getValue(), typeArg);
                checkSupported(topic, conf);
                inputs.add(isQueue()
                        ? new QueueInput(topic, subscribeQueue(topic, conf))
                        : new StreamInput(topic, subscribeStream(topic, conf)));
            }
        } catch (Exception e) {
            close();
            throw e;
        }
        inputs.forEach(Input::start);
    }

    private boolean isQueue() {
        return pulsarSourceConfig.getProcessingGuarantees() != FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE
                && (pulsarSourceConfig.getSubscriptionType() == null
                || pulsarSourceConfig.getSubscriptionType() == SubscriptionType.Shared);
    }

    private static void checkSupported(String topic, PulsarSourceConsumerConfig<?> conf) {
        if (conf.getConsumerProperties() != null && !conf.getConsumerProperties().isEmpty()) {
            throw new UnsupportedOperationException("Consumer properties are not supported with the V5 client, "
                    + "input topic " + topic);
        }
        if (conf.getCryptoKeyReader() != null) {
            throw new UnsupportedOperationException("Consumer encryption is not supported with the V5 client yet, "
                    + "input topic " + topic);
        }
        if (conf.getMessagePayloadProcessor() != null) {
            throw new UnsupportedOperationException("Message payload processors are not supported with the V5 "
                    + "client, input topic " + topic);
        }
        if (conf.isPoolMessages()) {
            log.warn().attr("topic", topic).log("poolMessages has no effect with the V5 client");
        }
    }

    private SubscriptionInitialPosition initialPosition() {
        return pulsarSourceConfig.getSubscriptionPosition()
                == org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest
                ? SubscriptionInitialPosition.EARLIEST
                : SubscriptionInitialPosition.LATEST;
    }

    private QueueConsumer<T> subscribeQueue(String topic, PulsarSourceConsumerConfig<T> conf)
            throws PulsarClientException {
        QueueConsumerBuilder<T> builder = clientV5.get().newQueueConsumer(V5Interop.toV5Schema(conf.getSchema()))
                .topic(topic)
                .subscriptionName(pulsarSourceConfig.getSubscriptionName())
                .subscriptionInitialPosition(initialPosition())
                .consumerName(consumerName)
                .properties(properties);
        if (conf.getReceiverQueueSize() != null) {
            builder.receiverQueueSize(conf.getReceiverQueueSize());
        }
        if (pulsarSourceConfig.getTimeoutMs() != null) {
            builder.processingTimeout(ProcessingTimeoutPolicy.of(Duration.ofMillis(pulsarSourceConfig.getTimeoutMs())));
        }
        Long negativeAckDelayMs = pulsarSourceConfig.getNegativeAckRedeliveryDelayMs();
        if (negativeAckDelayMs != null && negativeAckDelayMs > 0) {
            Duration delay = Duration.ofMillis(negativeAckDelayMs);
            builder.negativeAckRedeliveryBackoff(BackoffPolicy.fixed(delay, delay));
        }
        if (pulsarSourceConfig.getMaxMessageRetries() != null && pulsarSourceConfig.getMaxMessageRetries() >= 0) {
            DeadLetterPolicy.Builder deadLetterPolicy = DeadLetterPolicy.builder()
                    .maxRedeliverCount(pulsarSourceConfig.getMaxMessageRetries());
            if (pulsarSourceConfig.getDeadLetterTopic() != null && !pulsarSourceConfig.getDeadLetterTopic().isEmpty()) {
                deadLetterPolicy.deadLetterTopic(pulsarSourceConfig.getDeadLetterTopic());
            }
            builder.deadLetterPolicy(deadLetterPolicy.build());
        }
        log.info().attr("topic", topic).attr("subscription", pulsarSourceConfig.getSubscriptionName())
                .log("Subscribing with a V5 queue consumer");
        return builder.subscribe();
    }

    private StreamConsumer<T> subscribeStream(String topic, PulsarSourceConsumerConfig<T> conf)
            throws PulsarClientException {
        log.info().attr("topic", topic).attr("subscription", pulsarSourceConfig.getSubscriptionName())
                .log("Subscribing with a V5 stream consumer");
        return clientV5.get().newStreamConsumer(V5Interop.toV5Schema(conf.getSchema()))
                .topic(topic)
                .subscriptionName(pulsarSourceConfig.getSubscriptionName())
                .subscriptionInitialPosition(initialPosition())
                .consumerName(consumerName)
                .properties(properties)
                .subscribe();
    }

    @Override
    public void close() throws Exception {
        closed = true;
        for (Input input : inputs) {
            try {
                input.close();
            } catch (PulsarClientException e) {
                log.warn().attr("topic", input.topic).exception(e).log("Failed to close the V5 consumer");
            }
        }
        for (Input input : inputs) {
            if (input.receiveThread != null) {
                input.receiveThread.interrupt();
                input.receiveThread.join(CLOSE_TIMEOUT_MS);
            }
        }
    }

    /**
     * The V5 consumers are not v4 {@link Consumer}s, so there are none to expose for seek, pause and resume.
     */
    @Override
    public List<Consumer<T>> getInputConsumers() {
        return Collections.emptyList();
    }
}
