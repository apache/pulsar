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
package org.apache.pulsar.policies.data.loadbalancer;

/**
 * this class represents usages of resources used by a namespace, this is an aggregate of all the topics and queues on
 * the namespace. The class is designed from load balancing perspective so it only tracks resources that matter to load
 * Manager and the metrics are calculated how load manager prefers it.
 *
 */
public class NamespaceUsage {
    /** Total rate of messages produced on the broker (msg/s). */
    private double msgRateIn;

    /** Total throughput of messages produced on the broker (byte/s). */
    private double msgThroughputIn;

    /** Rate of persistent messages produced on the broker (msg/s). */
    private double msgPersistentRateIn;

    /** Throughput of persistent messages produced on the broker (byte/s). */
    private double msgPersistentThroughputIn;

    /** Rate of non-persistent messages produced on the broker (msg/s). */
    private double msgNonPersistentRateIn;

    /** Throughput of non-persistent messages produced on the broker (byte/s). */
    private double msgNonPersistentThroughputIn;

    /** Total rate of messages consumed from the broker (msg/s). */
    private double msgRateOut;

    /** Total throughput of messages consumed from the broker (byte/s). */
    private double msgThroughputOut;

    /** Number of messages in backlog for the broker. */
    private long msgBacklog;

    /** Space used to store the messages for the broker (bytes). */
    private long storageSize;

    /** Total number of producers = producer(queues) + producer(topics). */
    private long totalProducers;

    /** Number of clusters the namespace is replicated on. */
    private long totalReplicatedClusters;

    /** Total number of queues. */
    private long totalQueues;

    /** Total number of topics. */
    private long totalTopics;

    private long activeSubscribers;

    private long totalSubscribers;

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

    public double getMsgPersistentRateIn() {
        return msgPersistentRateIn;
    }

    public void setMsgPersistentRateIn(double msgPersistentRateIn) {
        this.msgPersistentRateIn = msgPersistentRateIn;
    }

    public double getMsgPersistentThroughputIn() {
        return msgPersistentThroughputIn;
    }

    public void setMsgPersistentThroughputIn(double msgPersistentThroughputIn) {
        this.msgPersistentThroughputIn = msgPersistentThroughputIn;
    }

    public double getMsgNonPersistentRateIn() {
        return msgNonPersistentRateIn;
    }

    public void setMsgNonPersistentRateIn(double msgNonPersistentRateIn) {
        this.msgNonPersistentRateIn = msgNonPersistentRateIn;
    }

    public double getMsgNonPersistentThroughputIn() {
        return msgNonPersistentThroughputIn;
    }

    public void setMsgNonPersistentThroughputIn(double msgNonPersistentThroughputIn) {
        this.msgNonPersistentThroughputIn = msgNonPersistentThroughputIn;
    }

    public double getMsgRateOut() {
        return msgRateOut;
    }

    public void setMsgRateOut(double msgRateOut) {
        this.msgRateOut = msgRateOut;
    }

    public double getMsgThroughputOut() {
        return msgThroughputOut;
    }

    public void setMsgThroughputOut(double msgThroughputOut) {
        this.msgThroughputOut = msgThroughputOut;
    }

    public long getMsgBacklog() {
        return msgBacklog;
    }

    public void setMsgBacklog(long msgBacklog) {
        this.msgBacklog = msgBacklog;
    }

    public long getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(long storageSize) {
        this.storageSize = storageSize;
    }

    public long getTotalProducers() {
        return totalProducers;
    }

    public void setTotalProducers(long totalProducers) {
        this.totalProducers = totalProducers;
    }

    public long getTotalReplicatedClusters() {
        return totalReplicatedClusters;
    }

    public void setTotalReplicatedClusters(long totalReplicatedClusters) {
        this.totalReplicatedClusters = totalReplicatedClusters;
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
