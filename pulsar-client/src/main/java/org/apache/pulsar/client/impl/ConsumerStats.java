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
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;

public class ConsumerStats implements Serializable {

    private static final long serialVersionUID = 1L;
    private TimerTask stat;
    private Timeout statTimeout;
    private ConsumerImpl consumer;
    private PulsarClientImpl pulsarClient;
    private long oldTime;
    private long statsIntervalSeconds;
    private final LongAdder numMsgsReceived;
    private final LongAdder numBytesReceived;
    private final LongAdder numReceiveFailed;
    private final LongAdder numAcksSent;
    private final LongAdder numAcksFailed;
    private final LongAdder totalMsgsReceived;
    private final LongAdder totalBytesReceived;
    private final LongAdder totalReceiveFailed;
    private final LongAdder totalAcksSent;
    private final LongAdder totalAcksFailed;

    private final DecimalFormat throughputFormat;

    public static final ConsumerStats CONSUMER_STATS_DISABLED = new ConsumerStatsDisabled();

    public ConsumerStats() {
        numMsgsReceived = null;
        numBytesReceived = null;
        numReceiveFailed = null;
        numAcksSent = null;
        numAcksFailed = null;
        totalMsgsReceived = null;
        totalBytesReceived = null;
        totalReceiveFailed = null;
        totalAcksSent = null;
        totalAcksFailed = null;
        throughputFormat = null;
    }

    public ConsumerStats(PulsarClientImpl pulsarClient, ConsumerConfiguration conf, ConsumerImpl consumer) {
        this.pulsarClient = pulsarClient;
        this.consumer = consumer;
        this.statsIntervalSeconds = pulsarClient.getConfiguration().getStatsIntervalSeconds();
        numMsgsReceived = new LongAdder();
        numBytesReceived = new LongAdder();
        numReceiveFailed = new LongAdder();
        numAcksSent = new LongAdder();
        numAcksFailed = new LongAdder();
        totalMsgsReceived = new LongAdder();
        totalBytesReceived = new LongAdder();
        totalReceiveFailed = new LongAdder();
        totalAcksSent = new LongAdder();
        totalAcksFailed = new LongAdder();
        throughputFormat = new DecimalFormat("0.00");
        init(conf);
    }

    private void init(ConsumerConfiguration conf) {
        ObjectMapper m = new ObjectMapper();
        m.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();

        try {
            log.info("Starting Pulsar consumer perf with config: {}", w.writeValueAsString(conf));
            log.info("Pulsar client config: {}", w.writeValueAsString(pulsarClient.getConfiguration()));
        } catch (IOException e) {
            log.error("Failed to dump config info: {}", e);
        }

        stat = (timeout) -> {
            if (timeout.isCancelled()) {
                return;
            }
            try {
                long now = System.nanoTime();
                double elapsed = (now - oldTime) / 1e9;
                oldTime = now;
                long currentNumMsgsReceived = numMsgsReceived.sumThenReset();
                long currentNumBytesReceived = numBytesReceived.sumThenReset();
                long currentNumReceiveFailed = numReceiveFailed.sumThenReset();
                long currentNumAcksSent = numAcksSent.sumThenReset();
                long currentNumAcksFailed = numAcksFailed.sumThenReset();

                totalMsgsReceived.add(currentNumMsgsReceived);
                totalBytesReceived.add(currentNumBytesReceived);
                totalReceiveFailed.add(currentNumReceiveFailed);
                totalAcksSent.add(currentNumAcksSent);
                totalAcksFailed.add(currentNumAcksFailed);

                if ((currentNumMsgsReceived | currentNumBytesReceived | currentNumReceiveFailed | currentNumAcksSent
                        | currentNumAcksFailed) != 0) {
                    log.info(
                            "[{}] [{}] [{}] Prefetched messages: {} --- Consume throughput: {} msgs/s --- "
                                    + "Throughput received: {} msg/s --- {} Mbit/s --- "
                                    + "Ack sent rate: {} ack/s --- " + "Failed messages: {} --- " + "Failed acks: {}",
                            consumer.getTopic(), consumer.getSubscription(), consumer.consumerName,
                            consumer.incomingMessages.size(), throughputFormat.format(currentNumMsgsReceived / elapsed),
                            throughputFormat.format(currentNumBytesReceived / elapsed * 8 / 1024 / 1024),
                            throughputFormat.format(currentNumAcksSent / elapsed), currentNumReceiveFailed,
                            currentNumAcksFailed);
                }
            } catch (Exception e) {
                log.error("[{}] [{}] [{}]: {}", consumer.getTopic(), consumer.subscription, consumer.consumerName,
                        e.getMessage());
            } finally {
                // schedule the next stat info
                statTimeout = pulsarClient.timer().newTimeout(stat, statsIntervalSeconds, TimeUnit.SECONDS);
            }
        };

        oldTime = System.nanoTime();
        statTimeout = pulsarClient.timer().newTimeout(stat, statsIntervalSeconds, TimeUnit.SECONDS);
    }

    void updateNumMsgsReceived(Message message) {
        if (message != null) {
            numMsgsReceived.increment();
            numBytesReceived.add(message.getData().length);
        }
    }

    void incrementNumAcksSent(long numAcks) {
        numAcksSent.add(numAcks);
    }

    void incrementNumAcksFailed() {
        numAcksFailed.increment();
    }

    void incrementNumReceiveFailed() {
        numReceiveFailed.increment();
    }

    Timeout getStatTimeout() {
        return statTimeout;
    }

    void reset() {
        numMsgsReceived.reset();
        numBytesReceived.reset();
        numReceiveFailed.reset();
        numAcksSent.reset();
        numAcksFailed.reset();
        totalMsgsReceived.reset();
        totalBytesReceived.reset();
        totalReceiveFailed.reset();
        totalAcksSent.reset();
        totalAcksFailed.reset();
    }

    void updateCumulativeStats(ConsumerStats stats) {
        if (stats == null) {
            return;
        }
        numMsgsReceived.add(stats.numMsgsReceived.longValue());
        numBytesReceived.add(stats.numBytesReceived.longValue());
        numReceiveFailed.add(stats.numReceiveFailed.longValue());
        numAcksSent.add(stats.numAcksSent.longValue());
        numAcksFailed.add(stats.numAcksFailed.longValue());
        totalMsgsReceived.add(stats.totalMsgsReceived.longValue());
        totalBytesReceived.add(stats.totalBytesReceived.longValue());
        totalReceiveFailed.add(stats.totalReceiveFailed.longValue());
        totalAcksSent.add(stats.totalAcksSent.longValue());
        totalAcksFailed.add(stats.totalAcksFailed.longValue());
    }

    public long getNumMsgsReceived() {
        return numMsgsReceived.longValue();
    }

    public long getNumBytesReceived() {
        return numBytesReceived.longValue();
    }

    public long getNumAcksSent() {
        return numAcksSent.longValue();
    }

    public long getNumAcksFailed() {
        return numAcksFailed.longValue();
    }

    public long getNumReceiveFailed() {
        return numReceiveFailed.longValue();
    }

    public long getTotalMsgsReceived() {
        return totalMsgsReceived.longValue();
    }

    public long getTotalBytesReceived() {
        return totalBytesReceived.longValue();
    }

    public long getTotalReceivedFailed() {
        return totalReceiveFailed.longValue();
    }

    public long getTotalAcksSent() {
        return totalAcksSent.longValue();
    }

    public long getTotalAcksFailed() {
        return totalAcksFailed.longValue();
    }

    private static final Logger log = LoggerFactory.getLogger(ConsumerStats.class);
}