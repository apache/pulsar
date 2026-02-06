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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAdder;
import org.apache.pulsar.client.api.PartitionedTopicProducerStats;
import org.apache.pulsar.client.api.ProducerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionedTopicProducerStatsRecorderImpl extends ProducerStatsRecorderImpl
        implements PartitionedTopicProducerStats {

    private static final long serialVersionUID = 1L;
    private Map<String, ProducerStats> partitionStats = Collections.emptyMap();
    private final DoubleAdder sendMsgsRateAggregate;
    private final DoubleAdder sendBytesRateAggregate;
    private int partitions = 0;
    private int pendingQueueSize;

    public PartitionedTopicProducerStatsRecorderImpl() {
        super();
        partitionStats = new ConcurrentHashMap<>();
        sendMsgsRateAggregate = new DoubleAdder();
        sendBytesRateAggregate = new DoubleAdder();
    }

    void reset() {
        super.reset();
        partitions = 0;
        pendingQueueSize = 0;
    }

    void updateCumulativeStats(String partition, ProducerStats stats) {
        super.updateCumulativeStats(stats);
        if (stats == null) {
            return;
        }
        partitionStats.put(partition, stats);
        // update rates
        sendMsgsRateAggregate.add(stats.getSendMsgsRate());
        sendBytesRateAggregate.add(stats.getSendBytesRate());
        partitions++;
        pendingQueueSize += stats.getPendingQueueSize();
    }

    @Override
    public double getSendMsgsRate() {
        return sendMsgsRateAggregate.doubleValue() / partitions;
    }

    @Override
    public double getSendBytesRate() {
        return sendBytesRateAggregate.doubleValue() / partitions;
    }

    @Override
    public Map<String, ProducerStats> getPartitionStats() {
        return partitionStats;
    }

    @Override
    public int getPendingQueueSize() {
        return pendingQueueSize;
    }

    private static final Logger log = LoggerFactory.getLogger(PartitionedTopicProducerStatsRecorderImpl.class);
}
