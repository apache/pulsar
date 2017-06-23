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
package org.apache.pulsar.broker.loadbalance.impl;

import org.apache.pulsar.broker.loadbalance.ServiceRequest;

public class PulsarServiceRequest extends ServiceRequest {
    private double msgRateIn;
    private double msgThroughputIn;
    private double msgRateOut;
    private double getMsgThroughputOut;
    private long msgBacklog;
    private long totalProducers;
    private long activeSubscribers;
    private long totalSubscribers;
    private long totalQueues;
    private long totalTopics;

    public double getMsgRateIn() {
        return msgRateIn;
    }

    public void setMsgRateIn(double msgRateIn) {
        this.msgRateIn = msgRateIn;
    }

    public double getMsgThroughputIn() {
        return msgThroughputIn;
    }

    public void setMsgThroughputIn(double msgThroughputIn) {
        this.msgThroughputIn = msgThroughputIn;
    }

    public double getMsgRateOut() {
        return msgRateOut;
    }

    public void setMsgRateOut(double msgRateOut) {
        this.msgRateOut = msgRateOut;
    }

    public double getGetMsgThroughputOut() {
        return getMsgThroughputOut;
    }

    public void setGetMsgThroughputOut(double getMsgThroughputOut) {
        this.getMsgThroughputOut = getMsgThroughputOut;
    }

    public long getMsgBacklog() {
        return msgBacklog;
    }

    public void setMsgBacklog(long msgBacklog) {
        this.msgBacklog = msgBacklog;
    }

    public long getTotalProducers() {
        return totalProducers;
    }

    public void setTotalProducers(long totalProducers) {
        this.totalProducers = totalProducers;
    }

    public long getActiveSubscribers() {
        return activeSubscribers;
    }

    public void setActiveSubscribers(long activeSubscribers) {
        this.activeSubscribers = activeSubscribers;
    }

    public long getTotalSubscribers() {
        return totalSubscribers;
    }

    public void setTotalSubscribers(long totalSubscribers) {
        this.totalSubscribers = totalSubscribers;
    }

    public long getTotalQueues() {
        return totalQueues;
    }

    public void setTotalQueues(long totalQueues) {
        this.totalQueues = totalQueues;
    }

    public long getTotalTopics() {
        return totalTopics;
    }

    public void setTotalTopics(long totalTopics) {
        this.totalTopics = totalTopics;
    }
}
