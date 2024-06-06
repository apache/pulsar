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
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
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

    public PersistentTopicAttributes(PersistentTopic topic) {
        super(topic);

        timeBasedQuotaAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .put(OpenTelemetryAttributes.PULSAR_BACKLOG_QUOTA_TYPE, "time")
                .build();
        sizeBasedQuotaAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .put(OpenTelemetryAttributes.PULSAR_BACKLOG_QUOTA_TYPE, "size")
                .build();

        transactionActiveAttributes =  Attributes.builder()
                .putAll(commonAttributes)
                .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_STATUS, "active")
                .build();
        transactionCommittedAttributes =  Attributes.builder()
                .putAll(commonAttributes)
                .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_STATUS, "committed")
                .build();
        transactionAbortedAttributes =  Attributes.builder()
                .putAll(commonAttributes)
                .put(OpenTelemetryAttributes.PULSAR_TRANSACTION_STATUS, "aborted")
                .build();

        compactionSuccessAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .put(OpenTelemetryAttributes.PULSAR_COMPACTION_STATUS, "success")
                .build();
        compactionFailureAttributes = Attributes.builder()
                .putAll(commonAttributes)
                .put(OpenTelemetryAttributes.PULSAR_COMPACTION_STATUS, "failure")
                .build();
    }
}
