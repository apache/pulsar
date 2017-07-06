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

import org.apache.pulsar.client.api.ProducerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.yahoo.sketches.quantiles.DoublesSketch;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;

public class ProducerStats implements Serializable {

    private static final long serialVersionUID = 1L;
    private TimerTask stat;
    private Timeout statTimeout;
    private ProducerImpl producer;
    private PulsarClientImpl pulsarClient;
    private long oldTime;
    private long statsIntervalSeconds;
    private final LongAdder numMsgsSent;
    private final LongAdder numBytesSent;
    private final LongAdder numSendFailed;
    private final LongAdder numAcksReceived;
    private final LongAdder totalMsgsSent;
    private final LongAdder totalBytesSent;
    private final LongAdder totalSendFailed;
    private final LongAdder totalAcksReceived;
    private final DecimalFormat dec;
    private final DecimalFormat throughputFormat;
    private final DoublesSketch ds;
    private final double[] percentiles = { 0.5, 0.95, 0.99, 0.999, 0.9999 };

    public static final ProducerStats PRODUCER_STATS_DISABLED = new ProducerStatsDisabled();

    public ProducerStats() {
        numMsgsSent = null;
        numBytesSent = null;
        numSendFailed = null;
        numAcksReceived = null;
        totalMsgsSent = null;
        totalBytesSent = null;
        totalSendFailed = null;
        totalAcksReceived = null;
        dec = null;
        throughputFormat = null;
        ds = null;
    }

    public ProducerStats(PulsarClientImpl pulsarClient, ProducerConfiguration conf, ProducerImpl producer) {
        this.pulsarClient = pulsarClient;
        this.statsIntervalSeconds = pulsarClient.getConfiguration().getStatsIntervalSeconds();
        this.producer = producer;
        numMsgsSent = new LongAdder();
        numBytesSent = new LongAdder();
        numSendFailed = new LongAdder();
        numAcksReceived = new LongAdder();
        totalMsgsSent = new LongAdder();
        totalBytesSent = new LongAdder();
        totalSendFailed = new LongAdder();
        totalAcksReceived = new LongAdder();
        ds = DoublesSketch.builder().build(256);
        dec = new DecimalFormat("0.000");
        throughputFormat = new DecimalFormat("0.00");
        init(conf);
    }

    private void init(ProducerConfiguration conf) {
        ObjectMapper m = new ObjectMapper();
        m.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        ObjectWriter w = m.writerWithDefaultPrettyPrinter();

        try {
            log.info("Starting Pulsar producer perf with config: {}", w.writeValueAsString(conf));
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

                long currentNumMsgsSent = numMsgsSent.sumThenReset();
                long currentNumBytesSent = numBytesSent.sumThenReset();
                long currentNumSendFailedMsgs = numSendFailed.sumThenReset();
                long currentNumAcksReceived = numAcksReceived.sumThenReset();

                totalMsgsSent.add(currentNumMsgsSent);
                totalBytesSent.add(currentNumBytesSent);
                totalSendFailed.add(currentNumSendFailedMsgs);
                totalAcksReceived.add(currentNumAcksReceived);

                double[] percentileValues;
                synchronized (ds) {
                    percentileValues = ds.getQuantiles(percentiles);
                    ds.reset();
                }

                if ((currentNumMsgsSent | currentNumSendFailedMsgs | currentNumAcksReceived
                        | currentNumMsgsSent) != 0) {

                    for (int i = 0; i < percentileValues.length; i++) {
                        if (percentileValues[i] == Double.NaN) {
                            percentileValues[i] = 0;
                        }
                    }

                    log.info(
                            "[{}] [{}] Pending messages: {} --- Publish throughput: {} msg/s --- {} Mbit/s --- "
                                    + "Latency: med: {} ms - 95pct: {} ms - 99pct: {} ms - 99.9pct: {} ms - 99.99pct: {} ms --- "
                                    + "Ack received rate: {} ack/s --- Failed messages: {}",
                            producer.getTopic(), producer.getProducerName(), producer.getPendingQueueSize(),
                            throughputFormat.format(currentNumMsgsSent / elapsed),
                            throughputFormat.format(currentNumBytesSent / elapsed / 1024 / 1024 * 8),
                            dec.format(percentileValues[0] / 1000.0), dec.format(percentileValues[1] / 1000.0),
                            dec.format(percentileValues[2] / 1000.0), dec.format(percentileValues[3] / 1000.0),
                            dec.format(percentileValues[4] / 1000.0),
                            throughputFormat.format(currentNumAcksReceived / elapsed), currentNumSendFailedMsgs);
                }

            } catch (Exception e) {
                log.error("[{}] [{}]: {}", producer.getTopic(), producer.getProducerName(), e.getMessage());
            } finally {
                // schedule the next stat info
                statTimeout = pulsarClient.timer().newTimeout(stat, statsIntervalSeconds, TimeUnit.SECONDS);
            }

        };

        oldTime = System.nanoTime();
        statTimeout = pulsarClient.timer().newTimeout(stat, statsIntervalSeconds, TimeUnit.SECONDS);
    }

    Timeout getStatTimeout() {
        return statTimeout;
    }

    void updateNumMsgsSent(long numMsgs, long totalMsgsSize) {
        numMsgsSent.add(numMsgs);
        numBytesSent.add(totalMsgsSize);
    }

    void incrementSendFailed() {
        numSendFailed.increment();
    }

    void incrementSendFailed(long numMsgs) {
        numSendFailed.add(numMsgs);
    }

    void incrementNumAcksReceived(long latencyNs) {
        numAcksReceived.increment();
        synchronized (ds) {
            ds.update(TimeUnit.NANOSECONDS.toMicros(latencyNs));
        }
    }

    void reset() {
        numMsgsSent.reset();
        numBytesSent.reset();
        numSendFailed.reset();
        numAcksReceived.reset();
        totalMsgsSent.reset();
        totalBytesSent.reset();
        totalSendFailed.reset();
        totalAcksReceived.reset();
    }

    void updateCumulativeStats(ProducerStats stats) {
        if (stats == null) {
            return;
        }
        numMsgsSent.add(stats.numMsgsSent.longValue());
        numBytesSent.add(stats.numBytesSent.longValue());
        numSendFailed.add(stats.numSendFailed.longValue());
        numAcksReceived.add(stats.numAcksReceived.longValue());
        totalMsgsSent.add(stats.numMsgsSent.longValue());
        totalBytesSent.add(stats.numBytesSent.longValue());
        totalSendFailed.add(stats.numSendFailed.longValue());
        totalAcksReceived.add(stats.numAcksReceived.longValue());
    }

    public long getNumMsgsSent() {
        return numMsgsSent.longValue();
    }

    public long getNumBytesSent() {
        return numBytesSent.longValue();
    }

    public long getNumSendFailed() {
        return numSendFailed.longValue();
    }

    public long getNumAcksReceived() {
        return numAcksReceived.longValue();
    }

    public long getTotalMsgsSent() {
        return totalMsgsSent.longValue();
    }

    public long getTotalBytesSent() {
        return totalBytesSent.longValue();
    }

    public long getTotalSendFailed() {
        return totalSendFailed.longValue();
    }

    public long getTotalAcksReceived() {
        return totalAcksReceived.longValue();
    }

    public void cancelStatsTimeout() {
        if (statTimeout != null) {
            statTimeout.cancel();
            statTimeout = null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerStats.class);
}
