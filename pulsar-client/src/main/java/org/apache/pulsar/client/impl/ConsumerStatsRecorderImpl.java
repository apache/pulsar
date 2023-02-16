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
package org.apache.pulsar.client.impl;

import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerStatsRecorderImpl implements ConsumerStatsRecorder {

    private static final long serialVersionUID = 1L;
    private TimerTask stat;
    private Timeout statTimeout;
    private final Consumer<?> consumer;
    private PulsarClientImpl pulsarClient;
    private long oldTime;
    private long statsIntervalSeconds;
    private final LongAdder numMsgsReceived;
    private final LongAdder numBytesReceived;
    private final LongAdder numReceiveFailed;
    private final LongAdder numBatchReceiveFailed;
    private final LongAdder numAcksSent;
    private final LongAdder numAcksFailed;
    private final LongAdder totalMsgsReceived;
    private final LongAdder totalBytesReceived;
    private final LongAdder totalReceiveFailed;
    private final LongAdder totalBatchReceiveFailed;
    private final LongAdder totalAcksSent;
    private final LongAdder totalAcksFailed;

    private volatile double receivedMsgsRate;
    private volatile double receivedBytesRate;

    private static final DecimalFormat THROUGHPUT_FORMAT = new DecimalFormat("0.00");

    public ConsumerStatsRecorderImpl() {
        this(null);
    }

    public ConsumerStatsRecorderImpl(Consumer<?> consumer) {
        this.consumer = consumer;
        numMsgsReceived = new LongAdder();
        numBytesReceived = new LongAdder();
        numReceiveFailed = new LongAdder();
        numBatchReceiveFailed = new LongAdder();
        numAcksSent = new LongAdder();
        numAcksFailed = new LongAdder();
        totalMsgsReceived = new LongAdder();
        totalBytesReceived = new LongAdder();
        totalReceiveFailed = new LongAdder();
        totalBatchReceiveFailed = new LongAdder();
        totalAcksSent = new LongAdder();
        totalAcksFailed = new LongAdder();
    }

    public ConsumerStatsRecorderImpl(PulsarClientImpl pulsarClient, ConsumerConfigurationData<?> conf,
            Consumer<?> consumer) {
        this.pulsarClient = pulsarClient;
        this.consumer = consumer;
        this.statsIntervalSeconds = pulsarClient.getConfiguration().getStatsIntervalSeconds();
        numMsgsReceived = new LongAdder();
        numBytesReceived = new LongAdder();
        numReceiveFailed = new LongAdder();
        numBatchReceiveFailed = new LongAdder();
        numAcksSent = new LongAdder();
        numAcksFailed = new LongAdder();
        totalMsgsReceived = new LongAdder();
        totalBytesReceived = new LongAdder();
        totalReceiveFailed = new LongAdder();
        totalBatchReceiveFailed = new LongAdder();
        totalAcksSent = new LongAdder();
        totalAcksFailed = new LongAdder();
        init(conf);
    }

    private void init(ConsumerConfigurationData<?> conf) {
        ObjectWriter w = ObjectMapperFactory.getMapperWithIncludeAlways().writer()
                .without(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        try {
            log.info("Starting Pulsar consumer status recorder with config: {}", w.writeValueAsString(conf));
            log.info("Pulsar client config: {}", w.writeValueAsString(pulsarClient.getConfiguration()));
        } catch (IOException e) {
            log.error("Failed to dump config info", e);
        }

        stat = (timeout) -> {
            if (timeout.isCancelled() || !(consumer instanceof ConsumerImpl)) {
                return;
            }
            ConsumerImpl<?> consumerImpl = (ConsumerImpl<?>) consumer;
            try {
                long now = System.nanoTime();
                double elapsed = (now - oldTime) / 1e9;
                oldTime = now;
                long currentNumMsgsReceived = numMsgsReceived.sumThenReset();
                long currentNumBytesReceived = numBytesReceived.sumThenReset();
                long currentNumReceiveFailed = numReceiveFailed.sumThenReset();
                long currentNumBatchReceiveFailed = numBatchReceiveFailed.sumThenReset();
                long currentNumAcksSent = numAcksSent.sumThenReset();
                long currentNumAcksFailed = numAcksFailed.sumThenReset();

                totalMsgsReceived.add(currentNumMsgsReceived);
                totalBytesReceived.add(currentNumBytesReceived);
                totalReceiveFailed.add(currentNumReceiveFailed);
                totalBatchReceiveFailed.add(currentNumBatchReceiveFailed);
                totalAcksSent.add(currentNumAcksSent);
                totalAcksFailed.add(currentNumAcksFailed);

                receivedMsgsRate = currentNumMsgsReceived / elapsed;
                receivedBytesRate = currentNumBytesReceived / elapsed;
                if ((currentNumMsgsReceived | currentNumBytesReceived | currentNumReceiveFailed | currentNumAcksSent
                        | currentNumAcksFailed) != 0) {
                    log.info(
                            "[{}] [{}] [{}] Prefetched messages: {} --- "
                                    + "Consume throughput received: {} msgs/s --- {} Mbit/s --- "
                                    + "Ack sent rate: {} ack/s --- " + "Failed messages: {} --- batch messages: {} ---"
                                    + "Failed acks: {}",
                            consumerImpl.getTopic(), consumerImpl.getSubscription(), consumerImpl.consumerName,
                            consumerImpl.incomingMessages.size(), THROUGHPUT_FORMAT.format(receivedMsgsRate),
                            THROUGHPUT_FORMAT.format(receivedBytesRate * 8 / 1024 / 1024),
                            THROUGHPUT_FORMAT.format(currentNumAcksSent / elapsed), currentNumReceiveFailed,
                            currentNumBatchReceiveFailed, currentNumAcksFailed);
                }
            } catch (Exception e) {
                log.error("[{}] [{}] [{}]: {}", consumerImpl.getTopic(), consumerImpl.subscription
                        , consumerImpl.consumerName, e.getMessage());
            } finally {
                // schedule the next stat info
                statTimeout = pulsarClient.timer().newTimeout(stat, statsIntervalSeconds, TimeUnit.SECONDS);
            }
        };

        oldTime = System.nanoTime();
        statTimeout = pulsarClient.timer().newTimeout(stat, statsIntervalSeconds, TimeUnit.SECONDS);
    }

    @Override
    public void updateNumMsgsReceived(Message<?> message) {
        if (message != null) {
            numMsgsReceived.increment();
            numBytesReceived.add(message.size());
        }
    }

    @Override
    public void incrementNumAcksSent(long numAcks) {
        numAcksSent.add(numAcks);
    }

    @Override
    public void incrementNumAcksFailed() {
        numAcksFailed.increment();
    }

    @Override
    public void incrementNumReceiveFailed() {
        numReceiveFailed.increment();
    }

    @Override
    public void incrementNumBatchReceiveFailed() {
        numBatchReceiveFailed.increment();
    }

    @Override
    public Optional<Timeout> getStatTimeout() {
        return Optional.ofNullable(statTimeout);
    }

    @Override
    public void reset() {
        numMsgsReceived.reset();
        numBytesReceived.reset();
        numReceiveFailed.reset();
        numBatchReceiveFailed.reset();
        numAcksSent.reset();
        numAcksFailed.reset();
        totalMsgsReceived.reset();
        totalBytesReceived.reset();
        totalReceiveFailed.reset();
        totalBatchReceiveFailed.reset();
        totalAcksSent.reset();
        totalAcksFailed.reset();
    }

    @Override
    public void updateCumulativeStats(ConsumerStats stats) {
        if (stats == null) {
            return;
        }
        numMsgsReceived.add(stats.getNumMsgsReceived());
        numBytesReceived.add(stats.getNumBytesReceived());
        numReceiveFailed.add(stats.getNumReceiveFailed());
        numBatchReceiveFailed.add(stats.getNumBatchReceiveFailed());
        numAcksSent.add(stats.getNumAcksSent());
        numAcksFailed.add(stats.getNumAcksFailed());
        totalMsgsReceived.add(stats.getTotalMsgsReceived());
        totalBytesReceived.add(stats.getTotalBytesReceived());
        totalReceiveFailed.add(stats.getTotalReceivedFailed());
        totalBatchReceiveFailed.add(stats.getTotaBatchReceivedFailed());
        totalAcksSent.add(stats.getTotalAcksSent());
        totalAcksFailed.add(stats.getTotalAcksFailed());
    }

    @Override
    public Integer getMsgNumInReceiverQueue() {
        if (consumer instanceof ConsumerBase) {
            return ((ConsumerBase<?>) consumer).incomingMessages.size();
        }
        return null;
    }

    @Override
    public Map<Long, Integer> getMsgNumInSubReceiverQueue() {
        if (consumer instanceof MultiTopicsConsumerImpl) {
            List<ConsumerImpl<?>> consumerList = ((MultiTopicsConsumerImpl) consumer).getConsumers();
            return consumerList.stream().collect(
                    Collectors.toMap((consumerImpl) -> consumerImpl.consumerId
                            , (consumerImpl) -> consumerImpl.incomingMessages.size())
            );
        }
        return null;
    }

    @Override
    public long getNumMsgsReceived() {
        return numMsgsReceived.longValue();
    }

    @Override
    public long getNumBytesReceived() {
        return numBytesReceived.longValue();
    }

    @Override
    public long getNumAcksSent() {
        return numAcksSent.longValue();
    }

    @Override
    public long getNumAcksFailed() {
        return numAcksFailed.longValue();
    }

    @Override
    public long getNumReceiveFailed() {
        return numReceiveFailed.longValue();
    }

    @Override
    public long getNumBatchReceiveFailed() {
        return numBatchReceiveFailed.longValue();
    }

    @Override
    public long getTotalMsgsReceived() {
        return totalMsgsReceived.longValue();
    }

    @Override
    public long getTotalBytesReceived() {
        return totalBytesReceived.longValue();
    }

    @Override
    public long getTotalReceivedFailed() {
        return totalReceiveFailed.longValue();
    }

    @Override
    public long getTotaBatchReceivedFailed() {
        return totalBatchReceiveFailed.longValue();
    }

    @Override
    public long getTotalAcksSent() {
        return totalAcksSent.longValue();
    }

    @Override
    public long getTotalAcksFailed() {
        return totalAcksFailed.longValue();
    }

    @Override
    public double getRateMsgsReceived() {
        return receivedMsgsRate;
    }

    @Override
    public double getRateBytesReceived() {
        return receivedBytesRate;
    }

    private static final Logger log = LoggerFactory.getLogger(ConsumerStatsRecorderImpl.class);
}
