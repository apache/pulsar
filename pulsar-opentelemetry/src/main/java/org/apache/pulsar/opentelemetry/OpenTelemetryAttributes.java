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
package org.apache.pulsar.opentelemetry;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import java.util.List;

/**
 * Common OpenTelemetry attributes to be used by Pulsar components.
 */
public interface OpenTelemetryAttributes {
    /**
     * The name of the Pulsar cluster. This attribute is automatically added to all signals by
     * {@link OpenTelemetryService}.
     */
    AttributeKey<String> PULSAR_CLUSTER = AttributeKey.stringKey("pulsar.cluster");

    /**
     * The name of the Pulsar namespace.
     */
    AttributeKey<String> PULSAR_NAMESPACE = AttributeKey.stringKey("pulsar.namespace");

    /**
     * The name of the Pulsar tenant.
     */
    AttributeKey<String> PULSAR_TENANT = AttributeKey.stringKey("pulsar.tenant");

    /**
     * The Pulsar topic domain.
     */
    AttributeKey<String> PULSAR_DOMAIN = AttributeKey.stringKey("pulsar.domain");

    /**
     * The name of the Pulsar topic.
     */
    AttributeKey<String> PULSAR_TOPIC = AttributeKey.stringKey("pulsar.topic");

    /**
     * The partition index of a Pulsar topic.
     */
    AttributeKey<Long> PULSAR_PARTITION_INDEX = AttributeKey.longKey("pulsar.partition.index");

    /**
     * The name of the Pulsar subscription.
     */
    AttributeKey<String> PULSAR_SUBSCRIPTION_NAME = AttributeKey.stringKey("pulsar.subscription.name");

    /**
     * The type of the Pulsar subscription.
     */
    AttributeKey<String> PULSAR_SUBSCRIPTION_TYPE = AttributeKey.stringKey("pulsar.subscription.type");

    /**
     * The name of the Pulsar consumer.
     */
    AttributeKey<String> PULSAR_CONSUMER_NAME = AttributeKey.stringKey("pulsar.consumer.name");

    /**
     * The ID of the Pulsar consumer.
     */
    AttributeKey<Long> PULSAR_CONSUMER_ID = AttributeKey.longKey("pulsar.consumer.id");

    /**
     * The consumer metadata properties, as a list of "key:value" pairs.
     */
    AttributeKey<List<String>> PULSAR_CONSUMER_METADATA = AttributeKey.stringArrayKey("pulsar.consumer.metadata");

    /**
     * The UTC timestamp of the Pulsar consumer creation.
     */
    AttributeKey<Long> PULSAR_CONSUMER_CONNECTED_SINCE = AttributeKey.longKey("pulsar.consumer.connected_since");

    /**
     * The address of the Pulsar client.
     */
    AttributeKey<String> PULSAR_CLIENT_ADDRESS = AttributeKey.stringKey("pulsar.client.address");

    /**
     * The version of the Pulsar client.
     */
    AttributeKey<String> PULSAR_CLIENT_VERSION = AttributeKey.stringKey("pulsar.client.version");

    /**
     * The status of the Pulsar transaction.
     */
    AttributeKey<String> PULSAR_TRANSACTION_STATUS = AttributeKey.stringKey("pulsar.transaction.status");
    enum TransactionStatus {
        ACTIVE,
        COMMITTED,
        ABORTED;
        public final Attributes attributes = Attributes.of(PULSAR_TRANSACTION_STATUS, name().toLowerCase());
    }

    /**
     * The status of the Pulsar compaction operation.
     */
    AttributeKey<String> PULSAR_COMPACTION_STATUS = AttributeKey.stringKey("pulsar.compaction.status");
    enum CompactionStatus {
        SUCCESS,
        FAILURE;
        public final Attributes attributes = Attributes.of(PULSAR_COMPACTION_STATUS, name().toLowerCase());
    }

    /**
     * The type of the backlog quota.
     */
    AttributeKey<String> PULSAR_BACKLOG_QUOTA_TYPE = AttributeKey.stringKey("pulsar.backlog.quota.type");
    enum BacklogQuotaType {
        SIZE,
        TIME;
        public final Attributes attributes = Attributes.of(PULSAR_BACKLOG_QUOTA_TYPE, name().toLowerCase());
    }
}
