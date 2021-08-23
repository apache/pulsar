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
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.pulsar.client.api.ProducerStats;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.yahoo.sketches.quantiles.DoublesSketch;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;

public class ProducerStatsRecorderImpl implements ProducerStatsRecorder {

    private static final long serialVersionUID = 1L;
    private transient TimerTask stat;
    private transient Timeout statTimeout;
    private transient ProducerImpl<?> producer;
    private transient PulsarClientImpl pulsarClient;
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
    private static final DecimalFormat DEC = new DecimalFormat("0.000");
    private static final DecimalFormat THROUGHPUT_FORMAT = new DecimalFormat("0.00");
    private transient final DoublesSketch ds;

    private volatile double sendMsgsRate;
    private volatile double sendBytesRate;
    private volatile double[] latencyPctValues = new double[PERCENTILES.length];

    private static final double[] PERCENTILES = { 0.5, 0.75, 0.95, 0.99, 0.999, 1.0 };

    public ProducerStatsRecorderImpl() {
        numMsgsSent = new LongAdder();
        numBytesSent = new LongAdder();
        numSendFailed = new LongAdder();
        numAcksReceived = new LongAdder();
        totalMsgsSent = new LongAdder();
        totalBytesSent = new LongAdder();
        totalSendFailed = new LongAdder();
        totalAcksReceived = new LongAdder();
        ds = DoublesSketch.builder().build(256);
    }

    public ProducerStatsRecorderImpl(PulsarClientImpl pulsarClient, ProducerConfigurationData conf,
            ProducerImpl<?> producer) {
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
        init(conf);
    }

    private void init(ProducerConfigurationData conf) {
        ObjectMapper m = new ObjectMapper();
        m.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        ObjectWriter w = m.writer();

        try {
            log.info("Starting Pulsar producer perf with config: {}", w.writeValueAsString(conf));
            log.info("Pulsar client config: {}", w.withoutAttribute("authentication").writeValueAsString(pulsarClient.getConfiguration()));
        } catch (IOException e) {
            log.error("Failed to dump config info", e);
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

                synchronized (ds) {
                    latencyPctValues = ds.getQuantiles(PERCENTILES);
                    ds.reset();
                }

                sendMsgsRate = currentNumMsgsSent / elapsed;
                sendBytesRate = currentNumBytesSent / elapsed;

                if ((currentNumMsgsSent | currentNumSendFailedMsgs | currentNumAcksReceived
                        | currentNumMsgsSent) != 0) {

                    for (int i = 0; i < latencyPctValues.length; i++) {
                        if (Double.isNaN(latencyPctValues[i])) {
                            latencyPctValues[i] = 0;
                        }
                    }

                    log.info("[{}] [{}] Pending messages: {} --- Publish throughput: {} msg/s --- {} Mbit/s --- "
                            + "Latency: med: {} ms - 95pct: {} ms - 99pct: {} ms - 99.9pct: {} ms - max: {} ms --- "
                            + "Ack received rate: {} ack/s --- Failed messages: {}", producer.getTopic(),
                            producer.getProducerName(), producer.getPendingQueueSize(),
                            THROUGHPUT_FORMAT.format(sendMsgsRate),
                            THROUGHPUT_FORMAT.format(sendBytesRate / 1024 / 1024 * 8),
                            DEC.format(latencyPctValues[0]), DEC.format(latencyPctValues[2]),
                            DEC.format(latencyPctValues[3]), DEC.format(latencyPctValues[4]),
                            DEC.format(latencyPctValues[5]),
                            THROUGHPUT_FORMAT.format(currentNumAcksReceived / elapsed), currentNumSendFailedMsgs);
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

    @Override
    public void updateNumMsgsSent(long numMsgs, long totalMsgsSize) {
        numMsgsSent.add(numMsgs);
        numBytesSent.add(totalMsgsSize);
    }

    @Override
    public void incrementSendFailed() {
        numSendFailed.increment();
    }

    @Override
    public void incrementSendFailed(long numMsgs) {
        numSendFailed.add(numMsgs);
    }

    @Override
    public void incrementNumAcksReceived(long latencyNs) {
        numAcksReceived.increment();
        synchronized (ds) {
            ds.update(TimeUnit.NANOSECONDS.toMillis(latencyNs));
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
        numMsgsSent.add(stats.getNumMsgsSent());
        numBytesSent.add(stats.getNumBytesSent());
        numSendFailed.add(stats.getNumSendFailed());
        numAcksReceived.add(stats.getNumAcksReceived());
        totalMsgsSent.add(stats.getTotalMsgsSent());
        totalBytesSent.add(stats.getTotalBytesSent());
        totalSendFailed.add(stats.getTotalSendFailed());
        totalAcksReceived.add(stats.getTotalAcksReceived());
    }

    @Override
    public long getNumMsgsSent() {
        return numMsgsSent.longValue();
    }

    @Override
    public long getNumBytesSent() {
        return numBytesSent.longValue();
    }

    @Override
    public long getNumSendFailed() {
        return numSendFailed.longValue();
    }

    @Override
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

    @Override
    public double getSendMsgsRate() {
        return sendMsgsRate;
    }

    @Override
    public double getSendBytesRate() {
        return sendBytesRate;
    }

    @Override
    public double getSendLatencyMillis50pct() {
        return latencyPctValues[0];
    }

    @Override
    public double getSendLatencyMillis75pct() {
        return latencyPctValues[1];
    }

    @Override
    public double getSendLatencyMillis95pct() {
        return latencyPctValues[2];
    }

    @Override
    public double getSendLatencyMillis99pct() {
        return latencyPctValues[3];
    }

    @Override
    public double getSendLatencyMillis999pct() {
        return latencyPctValues[4];
    }

    @Override
    public double getSendLatencyMillisMax() {
        return latencyPctValues[5];
    }

    public void cancelStatsTimeout() {
        if (statTimeout != null) {
            statTimeout.cancel();
            statTimeout = null;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ProducerStatsRecorderImpl.class);
}
