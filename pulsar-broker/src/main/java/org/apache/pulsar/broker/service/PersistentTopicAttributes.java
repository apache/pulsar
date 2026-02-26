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

import static org.apache.pulsar.broker.service.AbstractTopic.getCustomMetricLabelsMap;
import io.opentelemetry.api.common.Attributes;
import java.util.Map;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.opentelemetry.OpenTelemetryAttributes;

@Getter
public class PersistentTopicAttributes extends TopicAttributes {

    private final TopicName topicName;
    private final PulsarService pulsar;

    // Pre-built attributes with specific labels (without custom metric labels)
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

    public PersistentTopicAttributes(TopicName topicName, PulsarService pulsar) {
        super(topicName);
        this.topicName = topicName;
        this.pulsar = pulsar;

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

    /**
     * Converts a custom metric label key to OpenTelemetry format.
     * The conversion follows OpenTelemetry semantic conventions by converting to lowercase.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code custom_label} → {@code custom_label}</li>
     *   <li>{@code MY_LABEL} → {@code my_label}</li>
     * </ul>
     *
     * @param originalKey the original key of the custom metric label
     * @return the OpenTelemetry-compliant attribute key
     * @see <a href="https://opentelemetry.io/docs/specs/semconv/general/naming/">OpenTelemetry Naming Conventions</a>
     */
    public static String toOpenTelemetryAttributeKey(String originalKey) {
        return originalKey.toLowerCase();
    }

    /**
     * Get custom attributes (metric labels) for this topic.
     * This method dynamically fetches custom labels to ensure they are always up-to-date.
     * Only supported for persistent topics.
     *
     * @return attributes containing only custom metric labels, or empty attributes if not available
     */
    public Attributes getCustomAttributes() {
        if (pulsar == null || !pulsar.getConfiguration().isExposeCustomTopicMetricLabelsEnabled()) {
            return Attributes.empty();
        }
        Map<String, String> customLabels = getCustomMetricLabelsMap(pulsar, topicName);
        if (customLabels.isEmpty()) {
            return Attributes.empty();
        }
        var builder = Attributes.builder();
        customLabels.forEach((key, value) -> builder.put(toOpenTelemetryAttributeKey(key), value));
        return builder.build();
    }

    /**
     * Build attributes by merging custom attributes into the given base attributes.
     *
     * @param baseAttributes the base attributes to merge custom attributes into
     * @param customAttributes the custom attributes to merge
     * @return attributes with custom attributes merged
     */
    public Attributes buildAttributesWithCustomLabels(Attributes baseAttributes, Attributes customAttributes) {
        if (customAttributes.isEmpty()) {
            return baseAttributes;
        }
        return Attributes.builder()
                .putAll(baseAttributes)
                .putAll(customAttributes)
                .build();
    }
}
