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
package org.apache.pulsar.storm;

import static java.lang.String.format;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarSpout extends BaseRichSpout implements IMetric {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSpout.class);

    public static final String NO_OF_PENDING_FAILED_MESSAGES = "numberOfPendingFailedMessages";
    public static final String NO_OF_MESSAGES_RECEIVED = "numberOfMessagesReceived";
    public static final String NO_OF_MESSAGES_EMITTED = "numberOfMessagesEmitted";
    public static final String NO_OF_MESSAGES_FAILED = "numberOfMessagesFailed";
    public static final String MESSAGE_NOT_AVAILABLE_COUNT = "messageNotAvailableCount";
    public static final String NO_OF_PENDING_ACKS = "numberOfPendingAcks";
    public static final String CONSUMER_RATE = "consumerRate";
    public static final String CONSUMER_THROUGHPUT_BYTES = "consumerThroughput";

    private final ClientConfigurationData clientConf;
    private final PulsarSpoutConfiguration pulsarSpoutConf;
    private final ConsumerConfigurationData<byte[]> consumerConf;
    private final long failedRetriesTimeoutNano;
    private final int maxFailedRetries;
    private final ConcurrentMap<MessageId, MessageRetries> pendingMessageRetries = new ConcurrentHashMap<>();
    private final Queue<Message<byte[]>> failedMessages = new ConcurrentLinkedQueue<>();
    private final ConcurrentMap<String, Object> metricsMap = new ConcurrentHashMap<>();

    private SharedPulsarClient sharedPulsarClient;
    private String componentId;
    private String spoutId;
    private SpoutOutputCollector collector;
    private PulsarSpoutConsumer consumer;
    private volatile long messagesReceived = 0;
    private volatile long messagesEmitted = 0;
    private volatile long messagesFailed = 0;
    private volatile long messageNotAvailableCount = 0;
    private volatile long pendingAcks = 0;
    private volatile long messageSizeReceived = 0;

    public PulsarSpout(PulsarSpoutConfiguration pulsarSpoutConf) {
        this(pulsarSpoutConf, PulsarClient.builder());
    }
    
    public PulsarSpout(PulsarSpoutConfiguration pulsarSpoutConf, ClientBuilder clientBuilder) {
        this(pulsarSpoutConf, ((ClientBuilderImpl) clientBuilder).getClientConfigurationData().clone(),
                new ConsumerConfigurationData<byte[]>());
    }

    public PulsarSpout(PulsarSpoutConfiguration pulsarSpoutConf, ClientConfigurationData clientConfig,
            ConsumerConfigurationData<byte[]> consumerConfig) {
        Objects.requireNonNull(pulsarSpoutConf.getServiceUrl());
        Objects.requireNonNull(pulsarSpoutConf.getTopic());
        Objects.requireNonNull(pulsarSpoutConf.getSubscriptionName());
        Objects.requireNonNull(pulsarSpoutConf.getMessageToValuesMapper());

        checkNotNull(pulsarSpoutConf, "spout configuration can't be null");
        checkNotNull(clientConfig, "client configuration can't be null");
        checkNotNull(consumerConfig, "consumer configuration can't be null");
        this.clientConf = clientConfig;
        this.clientConf.setServiceUrl(pulsarSpoutConf.getServiceUrl());
        this.consumerConf = consumerConfig;
        this.pulsarSpoutConf = pulsarSpoutConf;
        this.failedRetriesTimeoutNano = pulsarSpoutConf.getFailedRetriesTimeout(TimeUnit.NANOSECONDS);
        this.maxFailedRetries = pulsarSpoutConf.getMaxFailedRetries();
    }

    @Override
    public void close() {
        try {
            LOG.info("[{}] Closing Pulsar consumer for topic {}", spoutId, pulsarSpoutConf.getTopic());
            
            if (pulsarSpoutConf.isAutoUnsubscribe()) {
                try {
                    consumer.unsubscribe();    
                }catch(PulsarClientException e) {
                    LOG.error("[{}] Failed to unsubscribe {} on topic {}", spoutId,
                            this.pulsarSpoutConf.getSubscriptionName(), pulsarSpoutConf.getTopic(), e);
                }
            }
            
            if (!pulsarSpoutConf.isSharedConsumerEnabled() && consumer != null) {
                consumer.close();
            }
            if (sharedPulsarClient != null) {
                sharedPulsarClient.close();
            }
            pendingMessageRetries.clear();
            failedMessages.clear();
        } catch (PulsarClientException e) {
            LOG.error("[{}] Error closing Pulsar consumer for topic {}", spoutId, pulsarSpoutConf.getTopic(), e);
        }
    }

    @Override
    public void ack(Object msgId) {
        if (msgId instanceof Message) {
            Message<?> msg = (Message<?>) msgId;
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Received ack for message {}", spoutId, msg.getMessageId());
            }
            consumer.acknowledgeAsync(msg);
            pendingMessageRetries.remove(msg.getMessageId());
            // we should also remove message from failedMessages but it will be eventually removed while emitting next
            // tuple
            --pendingAcks;
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId instanceof Message) {
            @SuppressWarnings("unchecked")
            Message<byte[]> msg = (Message<byte[]>) msgId;
            MessageId id = msg.getMessageId();
            LOG.warn("[{}] Error processing message {}", spoutId, id);

            // Since the message processing failed, we put it in the failed messages queue if there are more retries
            // remaining for the message
            MessageRetries messageRetries = pendingMessageRetries.computeIfAbsent(id, (k) -> new MessageRetries());
            if ((failedRetriesTimeoutNano < 0
                    || (messageRetries.getTimeStamp() + failedRetriesTimeoutNano) > System.nanoTime())
                    && (maxFailedRetries < 0 || messageRetries.numRetries < maxFailedRetries)) {
                // since we can retry again, we increment retry count and put it in the queue
                LOG.info("[{}] Putting message {} in the retry queue", spoutId, id);
                messageRetries.incrementAndGet();
                pendingMessageRetries.putIfAbsent(id, messageRetries);
                failedMessages.add(msg);
                --pendingAcks;
                messagesFailed++;
            } else {
                LOG.warn("[{}] Number of retries limit reached, dropping the message {}", spoutId, id);
                ack(msg);
            }
        }

    }

    /**
     * Emits a tuple received from the Pulsar consumer unless there are any failed messages
     */
    @Override
    public void nextTuple() {
        emitNextAvailableTuple();
    }

    /**
     * It makes sure that it emits next available non-tuple to topology unless consumer queue doesn't have any message
     * available. It receives message from consumer queue and converts it to tuple and emits to topology. if the
     * converted tuple is null then it tries to receives next message and perform the same until it finds non-tuple to
     * emit.
     */
    public void emitNextAvailableTuple() {
        // check if there are any failed messages to re-emit in the topology
        if(emitFailedMessage()) {
            return;
        }

        Message<byte[]> msg;
        // receive from consumer if no failed messages
        if (consumer != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Receiving the next message from pulsar consumer to emit to the collector", spoutId);
            }
            try {
                boolean done = false;
                while (!done) {
                    msg = consumer.receive(100, TimeUnit.MILLISECONDS);
                    if (msg != null) {
                        ++messagesReceived;
                        messageSizeReceived += msg.getData().length;
                        done = mapToValueAndEmit(msg);
                    } else {
                        // queue is empty and nothing to emit
                        done = true;
                        messageNotAvailableCount++;
                    }
                }
            } catch (PulsarClientException e) {
                LOG.error("[{}] Error receiving message from pulsar consumer", spoutId, e);
            }
        }
    }

    private boolean emitFailedMessage() {
        Message<byte[]> msg;

        while ((msg = failedMessages.peek()) != null) {
            MessageRetries messageRetries = pendingMessageRetries.get(msg.getMessageId());
            if (messageRetries != null) {
                // emit the tuple if retry doesn't need backoff else sleep with backoff time and return without doing
                // anything
                if (Backoff.shouldBackoff(messageRetries.getTimeStamp(), TimeUnit.NANOSECONDS,
                        messageRetries.getNumRetries(), clientConf.getInitialBackoffIntervalNanos(),
                        clientConf.getMaxBackoffIntervalNanos())) {
                    Utils.sleep(TimeUnit.NANOSECONDS.toMillis(clientConf.getInitialBackoffIntervalNanos()));
                } else {
                    // remove the message from the queue and emit to the topology, only if it should not be backedoff
                    LOG.info("[{}] Retrying failed message {}", spoutId, msg.getMessageId());
                    failedMessages.remove();
                    mapToValueAndEmit(msg);
                }
                return true;
            }

            // messageRetries is null because messageRetries is already acked and removed from pendingMessageRetries
            // then remove it from failed message queue as well.
            if(LOG.isDebugEnabled()) {
                LOG.debug("[{}]-{} removing {} from failedMessage because it's already acked",
                        pulsarSpoutConf.getTopic(), spoutId, msg.getMessageId());
            }
            failedMessages.remove();
            // try to find out next failed message
            continue;
        }
        return false;
    }

    @Override
    @SuppressWarnings({ "rawtypes" })
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.componentId = context.getThisComponentId();
        this.spoutId = String.format("%s-%s", componentId, context.getThisTaskId());
        this.collector = collector;
        pendingMessageRetries.clear();
        failedMessages.clear();
        try {
            consumer = createConsumer();
            LOG.info("[{}] Created a pulsar consumer on topic {} to receive messages with subscription {}", spoutId,
                    pulsarSpoutConf.getTopic(), pulsarSpoutConf.getSubscriptionName());
        } catch (PulsarClientException e) {
            LOG.error("[{}] Error creating pulsar consumer on topic {}", spoutId, pulsarSpoutConf.getTopic(), e);
            throw new IllegalStateException(format("Failed to initialize consumer for %s-%s : %s",
                    pulsarSpoutConf.getTopic(), pulsarSpoutConf.getSubscriptionName(), e.getMessage()), e);
        }
        context.registerMetric(String.format("PulsarSpoutMetrics-%s-%s", componentId, context.getThisTaskIndex()), this,
                pulsarSpoutConf.getMetricsTimeIntervalInSecs());
    }

    private PulsarSpoutConsumer createConsumer() throws PulsarClientException {
        sharedPulsarClient = SharedPulsarClient.get(componentId, clientConf);
        PulsarSpoutConsumer consumer;
        if (pulsarSpoutConf.isSharedConsumerEnabled()) {
            consumer = pulsarSpoutConf.isDurableSubscription()
                    ? new SpoutConsumer(sharedPulsarClient.getSharedConsumer(newConsumerConfiguration()))
                    : new SpoutReader(sharedPulsarClient.getSharedReader(newReaderConfiguration()));
        } else {
            try {
                consumer = pulsarSpoutConf.isDurableSubscription()
                        ? new SpoutConsumer(sharedPulsarClient.getClient()
                                .subscribeAsync(newConsumerConfiguration()).join())
                        : new SpoutReader(sharedPulsarClient.getClient()
                                .createReaderAsync(newReaderConfiguration()).join());
            } catch (CompletionException e) {
                throw (PulsarClientException) e.getCause();
            }
        }
        return consumer;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        pulsarSpoutConf.getMessageToValuesMapper().declareOutputFields(declarer);

    }

    private boolean mapToValueAndEmit(Message<byte[]> msg) {
        if (msg != null) {
            Values values = pulsarSpoutConf.getMessageToValuesMapper().toValues(msg);
            ++pendingAcks;
            if (values == null) {
                // since the mapper returned null, we can drop the message and ack it immediately
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Dropping message {}", spoutId, msg.getMessageId());
                }
                ack(msg);
            } else {
                if (values instanceof PulsarTuple) {
                    collector.emit(((PulsarTuple) values).getOutputStream(), values, msg);
                } else {
                    collector.emit(values, msg);
                }
                ++messagesEmitted;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Emitted message {} to the collector", spoutId, msg.getMessageId());
                }
                return true;
            }
        }
        return false;
    }

    public class MessageRetries {
        private final long timestampInNano;
        private int numRetries;

        public MessageRetries() {
            this.timestampInNano = System.nanoTime();
            this.numRetries = 0;
        }

        public long getTimeStamp() {
            return timestampInNano;
        }

        public int incrementAndGet() {
            return ++numRetries;
        }

        public int getNumRetries() {
            return numRetries;
        }
    }

    /**
     * Helpers for metrics
     */

    @SuppressWarnings({ "rawtypes" })
    ConcurrentMap getMetrics() {
        metricsMap.put(NO_OF_PENDING_FAILED_MESSAGES, (long) pendingMessageRetries.size());
        metricsMap.put(NO_OF_MESSAGES_RECEIVED, messagesReceived);
        metricsMap.put(NO_OF_MESSAGES_EMITTED, messagesEmitted);
        metricsMap.put(NO_OF_MESSAGES_FAILED, messagesFailed);
        metricsMap.put(MESSAGE_NOT_AVAILABLE_COUNT, messageNotAvailableCount);
        metricsMap.put(NO_OF_PENDING_ACKS, pendingAcks);
        metricsMap.put(CONSUMER_RATE, ((double) messagesReceived) / pulsarSpoutConf.getMetricsTimeIntervalInSecs());
        metricsMap.put(CONSUMER_THROUGHPUT_BYTES,
                ((double) messageSizeReceived) / pulsarSpoutConf.getMetricsTimeIntervalInSecs());
        return metricsMap;
    }

    void resetMetrics() {
        messagesReceived = 0;
        messagesEmitted = 0;
        messageSizeReceived = 0;
        messagesFailed = 0;
        messageNotAvailableCount = 0;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Object getValueAndReset() {
        ConcurrentMap metrics = getMetrics();
        resetMetrics();
        return metrics;
    }

    private ReaderConfigurationData<byte[]> newReaderConfiguration() {
        ReaderConfigurationData<byte[]> readerConf = new ReaderConfigurationData<>();
        readerConf.setTopicName(pulsarSpoutConf.getTopic());
        readerConf.setReaderName(pulsarSpoutConf.getSubscriptionName());
        readerConf.setStartMessageId(pulsarSpoutConf.getNonDurableSubscriptionReadPosition());
        if (this.consumerConf != null) {
            readerConf.setCryptoFailureAction(consumerConf.getCryptoFailureAction());
            readerConf.setCryptoKeyReader(consumerConf.getCryptoKeyReader());
            readerConf.setReadCompacted(consumerConf.isReadCompacted());
            readerConf.setReceiverQueueSize(consumerConf.getReceiverQueueSize());
        }
        return readerConf;
    }

    private ConsumerConfigurationData<byte[]> newConsumerConfiguration() {
        ConsumerConfigurationData<byte[]> consumerConf = this.consumerConf != null ? this.consumerConf
                : new ConsumerConfigurationData<>();
        consumerConf.setTopicNames(Collections.singleton(pulsarSpoutConf.getTopic()));
        consumerConf.setSubscriptionName(pulsarSpoutConf.getSubscriptionName());
        consumerConf.setSubscriptionType(pulsarSpoutConf.getSubscriptionType());
        return consumerConf;
    }

    static class SpoutConsumer implements PulsarSpoutConsumer {
        private Consumer<byte[]> consumer;

        public SpoutConsumer(Consumer<byte[]> consumer) {
            super();
            this.consumer = consumer;
        }
        
        @Override
        public Message<byte[]> receive(int timeout, TimeUnit unit) throws PulsarClientException {
            return consumer.receive(timeout, unit);
        }

        @Override
        public void acknowledgeAsync(Message<?> msg) {
            consumer.acknowledgeAsync(msg);
        }

        @Override
        public void close() throws PulsarClientException {
            consumer.close();
        }

        @Override
        public void unsubscribe() throws PulsarClientException {
            consumer.unsubscribe();
        }

    }

    static class SpoutReader implements PulsarSpoutConsumer {
        private Reader<byte[]> reader;

        public SpoutReader(Reader<byte[]> reader) {
            super();
            this.reader = reader;
        }

        @Override
        public Message<byte[]> receive(int timeout, TimeUnit unit) throws PulsarClientException {
            return reader.readNext(timeout, unit);
        }

        @Override
        public void acknowledgeAsync(Message<?> msg) {
            // No-op
        }

        @Override
        public void close() throws PulsarClientException {
            try {
                reader.close();
            } catch (IOException e) {
                throw new PulsarClientException(e);
            }
        }

        @Override
        public void unsubscribe() throws PulsarClientException {
            // No-op
        }
    }
}
