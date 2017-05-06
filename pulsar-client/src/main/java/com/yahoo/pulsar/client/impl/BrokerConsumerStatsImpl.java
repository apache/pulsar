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

import com.yahoo.pulsar.client.api.BrokerConsumerStats;
import com.yahoo.pulsar.client.api.SubscriptionType;
import com.yahoo.pulsar.common.api.proto.PulsarApi.CommandConsumerStatsResponse;

public class BrokerConsumerStatsImpl implements BrokerConsumerStats {

    private final String DELIMITER = ";";

    /** validTillInMs - Stats will be valid till this time. */
    private long validTillInMs = System.currentTimeMillis();

    /** Total rate of messages delivered to the consumer. msg/s */
    private double msgRateOut = 0;

    /** Total throughput delivered to the consumer. bytes/s */
    private double msgThroughputOut = 0;

    /** Total rate of messages redelivered by this consumer. msg/s */
    private double msgRateRedeliver = 0;

    /** Name of the consumer */
    private String consumerName = "";

    /** Number of available message permits for the consumer */
    private long availablePermits = 0;

    /** Number of unacknowledged messages for the consumer */
    private long unackedMessages = 0;

    /** Flag to verify if consumer is blocked due to reaching threshold of unacked messages */
    private boolean blockedConsumerOnUnackedMsgs = false;

    /** Address of this consumer */
    private String address = "";

    /** Timestamp of connection */
    private String connectedSince = "";

    /** Whether this subscription is Exclusive or Shared or Failover */
    private SubscriptionType subscriptionType = null;

    /** Total rate of messages expired on this subscription. msg/s */
    private double msgRateExpired = 0;

    /** Number of messages in the subscription backlog */
    private long msgBacklog = 0;

    public BrokerConsumerStatsImpl(CommandConsumerStatsResponse response) {
        super();
        this.validTillInMs = System.currentTimeMillis();
        this.msgRateOut = response.getMsgRateOut();
        this.msgThroughputOut = response.getMsgThroughputOut();
        this.msgRateRedeliver = response.getMsgRateRedeliver();
        this.consumerName = response.getConsumerName();
        this.availablePermits = response.getAvailablePermits();
        this.unackedMessages = response.getUnackedMessages();
        this.blockedConsumerOnUnackedMsgs = response.getBlockedConsumerOnUnackedMsgs();
        this.address = response.getAddress();
        this.connectedSince = response.getConnectedSince();
        this.subscriptionType = SubscriptionType.valueOf(response.getType());
        this.msgRateExpired = response.getMsgRateExpired();
        this.msgBacklog = response.getMsgBacklog();
    }

    public BrokerConsumerStatsImpl() {
    }

    public void setCacheTime(long timeInMs) {
        validTillInMs = System.currentTimeMillis() + timeInMs;
    }

    /** Returns true if the Message is Expired **/
    public synchronized boolean isValid() {
        return System.currentTimeMillis() <= validTillInMs;
    }

    public double getMsgRateOut() {
        return msgRateOut;
    }

    public double getMsgThroughputOut() {
        return msgThroughputOut;
    }

    public double getMsgRateRedeliver() {
        return msgRateRedeliver;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public long getAvailablePermits() {
        return availablePermits;
    }

    public long getUnackedMessages() {
        return unackedMessages;
    }

    public boolean isBlockedConsumerOnUnackedMsgs() {
        return blockedConsumerOnUnackedMsgs;
    }

    public String getAddress() {
        return address;
    }

    public String getConnectedSince() {
        return connectedSince;
    }

    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    public double getMsgRateExpired() {
        return msgRateExpired;
    }

    public long getMsgBacklog() {
        return msgBacklog;
    }

    public synchronized void add(BrokerConsumerStatsImpl stats) {
        this.validTillInMs = (validTillInMs > stats.validTillInMs) ? validTillInMs : stats.validTillInMs;
        this.msgRateOut += stats.msgRateOut;
        this.msgThroughputOut += stats.msgThroughputOut;
        this.msgRateRedeliver += stats.msgRateRedeliver;
        this.consumerName += stats.getConsumerName() + DELIMITER;
        this.availablePermits += stats.getAvailablePermits();
        this.unackedMessages += stats.unackedMessages;
        this.blockedConsumerOnUnackedMsgs |= stats.blockedConsumerOnUnackedMsgs;
        this.address += stats.address + DELIMITER;
        this.connectedSince += stats.connectedSince + DELIMITER;
        if (this.subscriptionType == null) {
            this.subscriptionType = stats.getSubscriptionType();
        }
        this.msgRateExpired += stats.msgRateExpired;
        this.msgBacklog += stats.msgBacklog;
    }

    @Override
    public String toString() {
        return "BrokerConsumerStats [validTillInMs=" + validTillInMs + ", msgRateOut=" + msgRateOut
                + ", msgThroughputOut=" + msgThroughputOut + ", msgRateRedeliver=" + msgRateRedeliver
                + ", consumerName=" + consumerName + ", availablePermits=" + availablePermits + ", unackedMessages="
                + unackedMessages + ", blockedConsumerOnUnackedMsgs=" + blockedConsumerOnUnackedMsgs + ", address="
                + address + ", connectedSince=" + connectedSince + ", type=" + subscriptionType + ", msgRateExpired="
                + msgRateExpired + ", msgBacklog=" + msgBacklog + "]";
    }
}
