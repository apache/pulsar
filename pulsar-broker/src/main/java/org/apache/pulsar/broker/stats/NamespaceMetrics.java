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
package org.apache.pulsar.broker.stats;

import io.opentelemetry.api.common.Attributes;
import java.util.concurrent.atomic.LongAdder;
import lombok.Getter;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

public class NamespaceMetrics implements InputMetrics, OutputMetrics {

    private final BrokerMetrics brokerMetrics;

    @Getter
    private final Attributes attributes;

    private final LongAdder topicCount = new LongAdder();
    private final LongAdder subscriptionCount = new LongAdder();
    private final LongAdder producerCount = new LongAdder();
    private final LongAdder consumerCount = new LongAdder();

    private final LongAdder messageInCount = new LongAdder();
    private final LongAdder byteInCount = new LongAdder();

    private final LongAdder messageOutCount = new LongAdder();
    private final LongAdder byteOutCount = new LongAdder();
    private final LongAdder messageAckCount = new LongAdder();

    private final LongAdder storageCount = new LongAdder();
    private final LongAdder storageLogicalCount = new LongAdder();
    private final LongAdder storageBacklogCount = new LongAdder();
    private final LongAdder storageOffloadCount = new LongAdder();

    private final LongAdder storageInCount = new LongAdder();
    private final LongAdder storageOutCount = new LongAdder();

    public NamespaceMetrics(NamespaceName namespace, BrokerService brokerService) {
        attributes = Attributes.of(OpenTelemetryAttributes.PULSAR_NAMESPACE, namespace.toString());
        brokerMetrics = brokerService.getBrokerMetrics();
    }

    public long getTopicCount() {
        return topicCount.sum();
    }

    public long getSubscriptionCount() {
        return subscriptionCount.sum();
    }

    public long getProducerCount() {
        return producerCount.sum();
    }

    public long getConsumerCount() {
        return consumerCount.sum();
    }

    @Override
    public void recordMessageIn(long messageCount, long byteCount) {
        brokerMetrics.recordMessageIn(messageCount, byteCount);
        messageInCount.add(messageCount);
        byteInCount.add(byteCount);
    }

    public long getMessageInCount() {
        return messageInCount.sum();
    }

    public long getByteInCount() {
        return byteInCount.sum();
    }

    public long getStorageCount() {
        return storageCount.sum();
    }

    public long getStorageLogicalCount() {
        return storageLogicalCount.sum();
    }

    public long getStorageBacklogCount() {
        return storageBacklogCount.sum();
    }

    public long getStorageOffloadCount() {
        return storageOffloadCount.sum();
    }

    public long getStorageInCount() {
        return storageInCount.sum();
    }

    public long getStorageOutCount() {
        return storageOutCount.sum();
    }

    @Override
    public void recordMessageOut(long messageCount, long byteCount) {
        brokerMetrics.recordMessageOut(messageCount, byteCount);
        messageOutCount.add(messageCount);
        byteOutCount.add(byteCount);
    }

    @Override
    public long getMessageOutCount() {
        return messageOutCount.sum();
    }

    @Override
    public long getByteOutCount() {
        return byteOutCount.sum();
    }

    @Override
    public void recordMessageAck(long ackCount) {
        brokerMetrics.recordMessageAck(ackCount);
        messageAckCount.add(ackCount);
    }

    @Override
    public long getMessageAckCount() {
        return messageAckCount.sum();
    }
}
