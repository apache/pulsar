/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.impl;

import java.util.List;

import com.google.common.collect.Lists;
import com.yahoo.pulsar.client.api.BrokerConsumerStats;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.client.api.PulsarClientException.InvalidConfigurationException;
import com.yahoo.pulsar.client.api.SubscriptionType;

public class PartitionedBrokerConsumerStatsImpl implements BrokerConsumerStats {

    private String DELIMITER = ";";

    List<BrokerConsumerStats> brokerConsumerStatsList;

    PartitionedBrokerConsumerStatsImpl() {
        brokerConsumerStatsList = Lists.newArrayList();
    }

    public synchronized boolean isValid() {
        return brokerConsumerStatsList.stream().reduce(true, (accumulated, stats) -> accumulated & stats.isValid(),
                (accumulated1, accumulated2) -> accumulated1 & accumulated2);
    }

    public double getMsgRateOut() {
        return brokerConsumerStatsList.stream().mapToDouble(stats -> stats.getMsgRateOut()).sum();
    }

    public double getMsgThroughputOut() {
        return brokerConsumerStatsList.stream().mapToDouble(stats -> stats.getMsgThroughputOut()).sum();
    }

    public double getMsgRateRedeliver() {
        return brokerConsumerStatsList.stream().mapToDouble(stats -> stats.getMsgRateRedeliver()).sum();
    }

    public String getConsumerName() {
        return brokerConsumerStatsList.stream().reduce("",
                (accumulator, stats) -> stats.getConsumerName() + DELIMITER + accumulator,
                (acc1, acc2) -> acc1 + DELIMITER + acc2);
    }

    public long getAvailablePermits() {
        return brokerConsumerStatsList.stream().mapToLong(stats -> stats.getAvailablePermits()).sum();
    }

    public long getUnackedMessages() {
        return brokerConsumerStatsList.stream().mapToLong(stats -> stats.getUnackedMessages()).sum();
    }

    public boolean isBlockedConsumerOnUnackedMsgs() {
        return brokerConsumerStatsList.stream().reduce(true,
                (accumulated, stats) -> accumulated & stats.isBlockedConsumerOnUnackedMsgs(),
                (accumulated1, accumulated2) -> accumulated1 & accumulated2);
    }

    public String getAddress() {
        return brokerConsumerStatsList.stream().reduce("",
                (accumulator, stats) -> stats.getAddress() + DELIMITER + accumulator,
                (acc1, acc2) -> acc1 + DELIMITER + acc2);
    }

    public String getConnectedSince() {
        return brokerConsumerStatsList.stream().reduce("",
                (accumulator, stats) -> stats.getConnectedSince() + DELIMITER + accumulator,
                (acc1, acc2) -> acc1 + DELIMITER + acc2);
    }

    public SubscriptionType getSubscriptionType() {
        if (brokerConsumerStatsList.size() > 0) {
            return brokerConsumerStatsList.get(0).getSubscriptionType();
        }
        return SubscriptionType.Exclusive;
    }

    public double getMsgRateExpired() {
        return brokerConsumerStatsList.stream().mapToDouble(stats -> stats.getMsgRateExpired()).sum();
    }

    public long getMsgBacklog() {
        return brokerConsumerStatsList.stream().mapToLong(stats -> stats.getMsgBacklog()).sum();
    }

    public synchronized void add(BrokerConsumerStats stats) {
        brokerConsumerStatsList.add(stats);
    }

    public synchronized BrokerConsumerStats get(int partitionIndex) throws InvalidConfigurationException {
        int size = brokerConsumerStatsList.size();
        if (partitionIndex < 0 || partitionIndex >= size) {
            throw new PulsarClientException.InvalidConfigurationException("partitionIndex [" + partitionIndex
                    + "] needs to be positive and less than brokerConsumerStatsList.size() [" + size + "]");
        }
        return brokerConsumerStatsList.get(partitionIndex);
    }

    @Override
    public String toString() {
        return "PartitionedBrokerConsumerStatsImpl [" + brokerConsumerStatsList.stream().reduce("",
                (acc, stats) -> stats + "," + acc, (acc1, acc2) -> acc1 + "," + acc2) + "]";
    }
}
