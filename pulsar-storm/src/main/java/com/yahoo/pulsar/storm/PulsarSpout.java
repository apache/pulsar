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
package com.yahoo.pulsar.storm;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.yahoo.pulsar.client.api.ClientConfiguration;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.MessageId;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.client.impl.Backoff;

import backtype.storm.metric.api.IMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class PulsarSpout extends BaseRichSpout implements IMetric {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSpout.class);

    public static final String NO_OF_PENDING_FAILED_MESSAGES = "numberOfPendingFailedMessages";
    public static final String NO_OF_MESSAGES_RECEIVED = "numberOfMessagesReceived";
    public static final String NO_OF_MESSAGES_EMITTED = "numberOfMessagesEmitted";
    public static final String NO_OF_PENDING_ACKS = "numberOfPendingAcks";
    public static final String CONSUMER_RATE = "consumerRate";
    public static final String CONSUMER_THROUGHPUT_BYTES = "consumerThroughput";

    private final ClientConfiguration clientConf;
    private final ConsumerConfiguration consumerConf;
    private final PulsarSpoutConfiguration pulsarSpoutConf;
    private final long failedRetriesTimeoutNano;
    private final int maxFailedRetries;
    private final ConcurrentMap<MessageId, MessageRetries> pendingMessageRetries = Maps.newConcurrentMap();
    private final Queue<Message> failedMessages = Queues.newConcurrentLinkedQueue();
    private final ConcurrentMap<String, Object> metricsMap = Maps.newConcurrentMap();

    private SharedPulsarClient sharedPulsarClient;
    private String componentId;
    private String spoutId;
    private SpoutOutputCollector collector;
    private Consumer consumer;
    private volatile long messagesReceived = 0;
    private volatile long messagesEmitted = 0;
    private volatile long pendingAcks = 0;
    private volatile long messageSizeReceived = 0;

    public PulsarSpout(PulsarSpoutConfiguration pulsarSpoutConf, ClientConfiguration clientConf) {
        this(pulsarSpoutConf, clientConf, new ConsumerConfiguration());
    }

    public PulsarSpout(PulsarSpoutConfiguration pulsarSpoutConf, ClientConfiguration clientConf,
            ConsumerConfiguration consumerConf) {
        this.clientConf = clientConf;
        this.consumerConf = consumerConf;
        Preconditions.checkNotNull(pulsarSpoutConf.getServiceUrl());
        Preconditions.checkNotNull(pulsarSpoutConf.getTopic());
        Preconditions.checkNotNull(pulsarSpoutConf.getSubscriptionName());
        Preconditions.checkNotNull(pulsarSpoutConf.getMessageToValuesMapper());
        this.pulsarSpoutConf = pulsarSpoutConf;
        this.failedRetriesTimeoutNano = pulsarSpoutConf.getFailedRetriesTimeout(TimeUnit.NANOSECONDS);
        this.maxFailedRetries = pulsarSpoutConf.getMaxFailedRetries();
    }

    @Override
    public void close() {
        try {
            LOG.info("[{}] Closing Pulsar consumer for topic {}", spoutId, pulsarSpoutConf.getTopic());
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
            Message msg = (Message) msgId;
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Received ack for message {}", spoutId, msg.getMessageId());
            }
            consumer.acknowledgeAsync(msg);
            pendingMessageRetries.remove(msg.getMessageId());
            --pendingAcks;
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId instanceof Message) {
            Message msg = (Message) msgId;
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
        Message msg;

        // check if there are any failed messages to re-emit in the topology
        msg = failedMessages.peek();
        if (msg != null) {
            MessageRetries messageRetries = pendingMessageRetries.get(msg.getMessageId());
            if (Backoff.shouldBackoff(messageRetries.getTimeStamp(), TimeUnit.NANOSECONDS,
                    messageRetries.getNumRetries())) {
                Utils.sleep(100);
            } else {
                // remove the message from the queue and emit to the topology, only if it should not be backedoff
                LOG.info("[{}] Retrying failed message {}", spoutId, msg.getMessageId());
                failedMessages.remove();
                mapToValueAndEmit(msg);
            }
            return;
        }

        // receive from consumer if no failed messages
        if (consumer != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[{}] Receiving the next message from pulsar consumer to emit to the collector", spoutId);
            }
            try {
                msg = consumer.receive(1, TimeUnit.SECONDS);
                if (msg != null) {
                    ++messagesReceived;
                    messageSizeReceived += msg.getData().length;
                }
                mapToValueAndEmit(msg);
            } catch (PulsarClientException e) {
                LOG.error("[{}] Error receiving message from pulsar consumer", spoutId, e);
            }
        }
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
            sharedPulsarClient = SharedPulsarClient.get(componentId, pulsarSpoutConf.getServiceUrl(), clientConf);
            if (pulsarSpoutConf.isSharedConsumerEnabled()) {
                consumer = sharedPulsarClient.getSharedConsumer(pulsarSpoutConf.getTopic(),
                        pulsarSpoutConf.getSubscriptionName(), consumerConf);
            } else {
                consumer = sharedPulsarClient.getClient().subscribe(pulsarSpoutConf.getTopic(),
                        pulsarSpoutConf.getSubscriptionName(), consumerConf);
            }
            LOG.info("[{}] Created a pulsar consumer on topic {} to receive messages with subscription {}", spoutId,
                    pulsarSpoutConf.getTopic(), pulsarSpoutConf.getSubscriptionName());
        } catch (PulsarClientException e) {
            LOG.error("[{}] Error creating pulsar consumer on topic {}", spoutId, pulsarSpoutConf.getTopic(), e);
        }
        context.registerMetric(String.format("PulsarSpoutMetrics-%s-%s", componentId, context.getThisTaskIndex()), this,
                pulsarSpoutConf.getMetricsTimeIntervalInSecs());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        pulsarSpoutConf.getMessageToValuesMapper().declareOutputFields(declarer);

    }

    private void mapToValueAndEmit(Message msg) {
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
                collector.emit(values, msg);
                ++messagesEmitted;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("[{}] Emitted message {} to the collector", spoutId, msg.getMessageId());
                }
            }
        }
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
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Object getValueAndReset() {
        ConcurrentMap metrics = getMetrics();
        resetMetrics();
        return metrics;
    }
}
