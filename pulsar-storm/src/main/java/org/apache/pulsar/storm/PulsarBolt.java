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

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClientException;

import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import static java.lang.String.format;

public class PulsarBolt extends BaseRichBolt implements IMetric {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PulsarBolt.class);

    public static final String NO_OF_MESSAGES_SENT = "numberOfMessagesSent";
    public static final String PRODUCER_RATE = "producerRate";
    public static final String PRODUCER_THROUGHPUT_BYTES = "producerThroughput";

    private final ClientConfiguration clientConf;
    private final ProducerConfiguration producerConf;
    private final PulsarBoltConfiguration pulsarBoltConf;
    private final ConcurrentMap<String, Object> metricsMap = Maps.newConcurrentMap();

    private SharedPulsarClient sharedPulsarClient;
    private String componentId;
    private String boltId;
    private OutputCollector collector;
    private Producer<byte[]> producer;
    private volatile long messagesSent = 0;
    private volatile long messageSizeSent = 0;

    public PulsarBolt(PulsarBoltConfiguration pulsarBoltConf, ClientConfiguration clientConf) {
        this(pulsarBoltConf, clientConf, new ProducerConfiguration());
    }

    public PulsarBolt(PulsarBoltConfiguration pulsarBoltConf, ClientConfiguration clientConf,
            ProducerConfiguration producerConf) {
        this.clientConf = clientConf;
        this.producerConf = producerConf;
        Preconditions.checkNotNull(pulsarBoltConf.getServiceUrl());
        Preconditions.checkNotNull(pulsarBoltConf.getTopic());
        Preconditions.checkNotNull(pulsarBoltConf.getTupleToMessageMapper());
        this.pulsarBoltConf = pulsarBoltConf;
    }

    @SuppressWarnings({ "rawtypes" })
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.componentId = context.getThisComponentId();
        this.boltId = String.format("%s-%s", componentId, context.getThisTaskId());
        this.collector = collector;
        try {
            sharedPulsarClient = SharedPulsarClient.get(componentId, pulsarBoltConf.getServiceUrl(), clientConf);
            producer = sharedPulsarClient.getSharedProducer(pulsarBoltConf.getTopic(), producerConf);
            LOG.info("[{}] Created a pulsar producer on topic {} to send messages", boltId, pulsarBoltConf.getTopic());
        } catch (PulsarClientException e) {
            LOG.error("[{}] Error initializing pulsar producer on topic {}", boltId, pulsarBoltConf.getTopic(), e);
            throw new IllegalStateException(
                    format("Failed to initialize producer for %s : %s", pulsarBoltConf.getTopic(), e.getMessage()), e);
        }
        context.registerMetric(String.format("PulsarBoltMetrics-%s-%s", componentId, context.getThisTaskIndex()), this,
                pulsarBoltConf.getMetricsTimeIntervalInSecs());
    }

    @Override
    public void execute(Tuple input) {
        if (TupleUtils.isTick(input)) {
            collector.ack(input);
            return;
        }
        try {
            if (producer != null) {
                // a message key can be provided in the mapper
                Message<byte[]> msg = pulsarBoltConf.getTupleToMessageMapper().toMessage(input);
                if (msg == null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("[{}] Cannot send null message, acking the collector", boltId);
                    }
                    collector.ack(input);
                } else {
                    final long messageSizeToBeSent = msg.getData().length;
                    producer.sendAsync(msg).handle((r, ex) -> {
                        synchronized (collector) {
                            if (ex != null) {
                                collector.reportError(ex);
                                collector.fail(input);
                                LOG.error("[{}] Message send failed", boltId, ex);

                            } else {
                                collector.ack(input);
                                ++messagesSent;
                                messageSizeSent += messageSizeToBeSent;
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("[{}] Message sent with id {}", boltId, msg.getMessageId());
                                }
                            }
                        }

                        return null;
                    });
                }
            }
        } catch (Exception e) {
            LOG.error("[{}] Message processing failed", boltId, e);
            collector.reportError(e);
            collector.fail(input);
        }
    }

    public void close() {
        try {
            LOG.info("[{}] Closing Pulsar producer on topic {}", boltId, pulsarBoltConf.getTopic());
            if (sharedPulsarClient != null) {
                sharedPulsarClient.close();
            }
        } catch (PulsarClientException e) {
            LOG.error("[{}] Error closing Pulsar producer on topic {}", boltId, pulsarBoltConf.getTopic(), e);
        }
    }

    @Override
    public void cleanup() {
        close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        pulsarBoltConf.getTupleToMessageMapper().declareOutputFields(declarer);
    }

    /**
     * Helpers for metrics
     */

    @SuppressWarnings({ "rawtypes" })
    ConcurrentMap getMetrics() {
        metricsMap.put(NO_OF_MESSAGES_SENT, messagesSent);
        metricsMap.put(PRODUCER_RATE, ((double) messagesSent) / pulsarBoltConf.getMetricsTimeIntervalInSecs());
        metricsMap.put(PRODUCER_THROUGHPUT_BYTES,
                ((double) messageSizeSent) / pulsarBoltConf.getMetricsTimeIntervalInSecs());
        return metricsMap;
    }

    void resetMetrics() {
        messagesSent = 0;
        messageSizeSent = 0;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Object getValueAndReset() {
        ConcurrentMap metrics = getMetrics();
        resetMetrics();
        return metrics;
    }
}
