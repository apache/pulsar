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
package org.apache.pulsar.broker.service;

import io.opentelemetry.api.common.Attributes;
import lombok.Getter;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

@Getter
public class PersistentTopicAttributes extends TopicAttributes {

    private final Attributes timeBasedQuotaAttributes;
    private final Attributes sizeBasedQuotaAttributes;

    private final Attributes compactionSuccessAttributes;
    private final Attributes compactionFailureAttributes;

    private final Attributes transactionActiveAttributes;
    private final Attributes transactionCommittedAttributes;
    private final Attributes transactionAbortedAttributes;

    private final Attributes transactionBufferClientCommitSucceededAttributes;
    private final Attributes transactionBufferClientCommitFailedAttributes;
    private final Attributes transactionBufferClientAbortSucceededAttributes;
    private final Attributes transactionBufferClientAbortFailedAttributes;

    public PersistentTopicAttributes(TopicName topicName) {
        super(topicName);

        timeBasedQuotaAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.BacklogQuotaType.TIME.attributes)
                .build();
        sizeBasedQuotaAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.BacklogQuotaType.SIZE.attributes)
                .build();

        transactionActiveAttributes =  Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.TransactionStatus.ACTIVE.attributes)
                .build();
        transactionCommittedAttributes =  Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.TransactionStatus.COMMITTED.attributes)
                .build();
        transactionAbortedAttributes =  Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.TransactionStatus.ABORTED.attributes)
                .build();

        transactionBufferClientCommitSucceededAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .remove(OpenTelemetryAttributes.PULSAR_DOMAIN)
                .putAll(OpenTelemetryAttributes.TransactionStatus.COMMITTED.attributes)
                .putAll(OpenTelemetryAttributes.TransactionBufferClientOperationStatus.SUCCESS.attributes)
                .build();
        transactionBufferClientCommitFailedAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .remove(OpenTelemetryAttributes.PULSAR_DOMAIN)
                .putAll(OpenTelemetryAttributes.TransactionStatus.COMMITTED.attributes)
                .putAll(OpenTelemetryAttributes.TransactionBufferClientOperationStatus.FAILURE.attributes)
                .build();
        transactionBufferClientAbortSucceededAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .remove(OpenTelemetryAttributes.PULSAR_DOMAIN)
                .putAll(OpenTelemetryAttributes.TransactionStatus.ABORTED.attributes)
                .putAll(OpenTelemetryAttributes.TransactionBufferClientOperationStatus.SUCCESS.attributes)
                .build();
        transactionBufferClientAbortFailedAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .remove(OpenTelemetryAttributes.PULSAR_DOMAIN)
                .putAll(OpenTelemetryAttributes.TransactionStatus.ABORTED.attributes)
                .putAll(OpenTelemetryAttributes.TransactionBufferClientOperationStatus.FAILURE.attributes)
                .build();

        compactionSuccessAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.CompactionStatus.SUCCESS.attributes)
                .build();
        compactionFailureAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .putAll(OpenTelemetryAttributes.CompactionStatus.FAILURE.attributes)
                .build();
    }
}
