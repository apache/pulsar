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
package org.apache.pulsar.broker.transaction.pendingack;

import io.opentelemetry.api.common.Attributes;
import lombok.Getter;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.TransactionPendingAckOperationStatus;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes.TransactionStatus;

@Getter
public class PendingAckHandleAttributes {

    private final Attributes commitSuccessAttributes;
    private final Attributes commitFailureAttributes;
    private final Attributes abortSuccessAttributes;
    private final Attributes abortFailureAttributes;

    public PendingAckHandleAttributes(String topic, String subscription) {
        var topicName = TopicName.get(topic);
        commitSuccessAttributes = getAttributes(topicName, subscription, TransactionStatus.COMMITTED,
                TransactionPendingAckOperationStatus.SUCCESS);
        commitFailureAttributes = getAttributes(topicName, subscription, TransactionStatus.COMMITTED,
                TransactionPendingAckOperationStatus.FAILURE);
        abortSuccessAttributes = getAttributes(topicName, subscription, TransactionStatus.ABORTED,
                TransactionPendingAckOperationStatus.SUCCESS);
        abortFailureAttributes = getAttributes(topicName, subscription, TransactionStatus.ABORTED,
                TransactionPendingAckOperationStatus.FAILURE);
    }

    private static Attributes getAttributes(TopicName topicName, String subscriptionName,
                                            TransactionStatus txStatus,
                                            TransactionPendingAckOperationStatus txAckStoreStatus) {
        var builder = Attributes.builder()
                .put(OpenTelemetryAttributes.PULSAR_SUBSCRIPTION_NAME, subscriptionName)
                .put(OpenTelemetryAttributes.PULSAR_TENANT, topicName.getTenant())
                .put(OpenTelemetryAttributes.PULSAR_NAMESPACE, topicName.getNamespace())
                .put(OpenTelemetryAttributes.PULSAR_TOPIC, topicName.getPartitionedTopicName())
                .putAll(txStatus.attributes)
                .putAll(txAckStoreStatus.attributes);
        if (topicName.isPartitioned()) {
            builder.put(OpenTelemetryAttributes.PULSAR_PARTITION_INDEX, topicName.getPartitionIndex());
        }
        return builder.build();
    }
}
