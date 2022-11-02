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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.MultiTopicConsumerStats;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiTopicConsumerStatsRecorderImpl extends ConsumerStatsRecorderImpl implements MultiTopicConsumerStats {

    private static final long serialVersionUID = 1L;
    private Map<String, ConsumerStats> partitionStats = new ConcurrentHashMap<>();

    public MultiTopicConsumerStatsRecorderImpl() {
        super();
    }

    public MultiTopicConsumerStatsRecorderImpl(Consumer<?> consumer) {
        super(consumer);
    }

    public MultiTopicConsumerStatsRecorderImpl(PulsarClientImpl pulsarClient, ConsumerConfigurationData<?> conf,
            Consumer<?> consumer) {
        super(pulsarClient, conf, consumer);
    }

    public void updateCumulativeStats(String partition, ConsumerStats stats) {
        super.updateCumulativeStats(stats);
        partitionStats.put(partition, stats);
    }

    @Override
    public Map<String, ConsumerStats> getPartitionStats() {
        return partitionStats;
    }

    private static final Logger log = LoggerFactory.getLogger(MultiTopicConsumerStatsRecorderImpl.class);
}
