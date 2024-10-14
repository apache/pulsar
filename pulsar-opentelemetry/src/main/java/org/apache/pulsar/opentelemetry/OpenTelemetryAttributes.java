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
     * The name of the Pulsar producer.
     */
    AttributeKey<String> PULSAR_PRODUCER_NAME = AttributeKey.stringKey("pulsar.producer.name");

    /**
     * The ID of the Pulsar producer.
     */
    AttributeKey<Long> PULSAR_PRODUCER_ID = AttributeKey.longKey("pulsar.producer.id");

    /**
     * The access mode of the Pulsar producer.
     */
    AttributeKey<String> PULSAR_PRODUCER_ACCESS_MODE = AttributeKey.stringKey("pulsar.producer.access_mode");

    /**
     * The address of the Pulsar client.
     */
    AttributeKey<String> PULSAR_CLIENT_ADDRESS = AttributeKey.stringKey("pulsar.client.address");

    /**
     * The version of the Pulsar client.
     */
    AttributeKey<String> PULSAR_CLIENT_VERSION = AttributeKey.stringKey("pulsar.client.version");

    AttributeKey<String> PULSAR_CONNECTION_RATE_LIMIT_OPERATION_NAME =
            AttributeKey.stringKey("pulsar.connection.rate_limit.operation.name");
    enum ConnectionRateLimitOperationName {
        PAUSED,
        RESUMED,
        THROTTLED,
        UNTHROTTLED;
        public final Attributes attributes =
                Attributes.of(PULSAR_CONNECTION_RATE_LIMIT_OPERATION_NAME, name().toLowerCase());
    }

    /**
     * The status of the Pulsar transaction.
     */
    AttributeKey<String> PULSAR_TRANSACTION_STATUS = AttributeKey.stringKey("pulsar.transaction.status");

    enum TransactionStatus {
        ABORTED,
        ACTIVE,
        COMMITTED,
        CREATED,
        TIMEOUT;
        public final Attributes attributes = Attributes.of(PULSAR_TRANSACTION_STATUS, name().toLowerCase());
    }

    /**
     * The status of the Pulsar transaction ack store operation.
     */
    AttributeKey<String> PULSAR_TRANSACTION_ACK_STORE_OPERATION_STATUS =
            AttributeKey.stringKey("pulsar.transaction.pending.ack.store.operation.status");
    enum TransactionPendingAckOperationStatus {
        SUCCESS,
        FAILURE;
        public final Attributes attributes =
                Attributes.of(PULSAR_TRANSACTION_ACK_STORE_OPERATION_STATUS, name().toLowerCase());
    }

    /**
     * The ID of the Pulsar transaction coordinator.
     */
    AttributeKey<Long> PULSAR_TRANSACTION_COORDINATOR_ID = AttributeKey.longKey("pulsar.transaction.coordinator.id");

    /**
     * The status of the Pulsar transaction buffer client operation.
     */
    AttributeKey<String> PULSAR_TRANSACTION_BUFFER_CLIENT_OPERATION_STATUS =
            AttributeKey.stringKey("pulsar.transaction.buffer.client.operation.status");
    enum TransactionBufferClientOperationStatus {
        SUCCESS,
        FAILURE;
        public final Attributes attributes =
                Attributes.of(PULSAR_TRANSACTION_BUFFER_CLIENT_OPERATION_STATUS, name().toLowerCase());
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

    // Managed Ledger Attributes
    /**
     * The name of the managed ledger.
     */
    AttributeKey<String> ML_LEDGER_NAME = AttributeKey.stringKey("pulsar.managed_ledger.name");

    /**
     * The name of the managed cursor.
     */
    AttributeKey<String> ML_CURSOR_NAME = AttributeKey.stringKey("pulsar.managed_ledger.cursor.name");

    /**
     * The status of the managed cursor operation.
     */
    AttributeKey<String> ML_CURSOR_OPERATION_STATUS =
            AttributeKey.stringKey("pulsar.managed_ledger.cursor.operation.status");
    enum ManagedCursorOperationStatus {
        SUCCESS,
        FAILURE;
        public final Attributes attributes = Attributes.of(ML_CURSOR_OPERATION_STATUS, name().toLowerCase());
    }

    AttributeKey<String> MANAGED_LEDGER_READ_INFLIGHT_USAGE =
            AttributeKey.stringKey("pulsar.managed_ledger.inflight.read.usage.state");
    enum InflightReadLimiterUtilization {
        USED,
        FREE;
        public final Attributes attributes = Attributes.of(MANAGED_LEDGER_READ_INFLIGHT_USAGE, name().toLowerCase());
    }

    /**
     * The name of the remote cluster for a Pulsar replicator.
     */
    AttributeKey<String> PULSAR_REPLICATION_REMOTE_CLUSTER_NAME =
            AttributeKey.stringKey("pulsar.replication.remote.cluster.name");

    AttributeKey<String> PULSAR_CONNECTION_STATUS = AttributeKey.stringKey("pulsar.connection.status");
    enum ConnectionStatus {
        ACTIVE,
        OPEN,
        CLOSE;
        public final Attributes attributes = Attributes.of(PULSAR_CONNECTION_STATUS, name().toLowerCase());
    }

    AttributeKey<String> PULSAR_CONNECTION_CREATE_STATUS =
            AttributeKey.stringKey("pulsar.connection.create.operation.status");
    enum ConnectionCreateStatus {
        SUCCESS,
        FAILURE;
        public final Attributes attributes = Attributes.of(PULSAR_CONNECTION_CREATE_STATUS, name().toLowerCase());
    }

    // Managed Ledger Attributes

    /**
     * The name of the managed ledger.
     */
    AttributeKey<String> ML_NAME = AttributeKey.stringKey("pulsar.managed_ledger.name");

    /**
     * The status of the managed ledger operation.
     */
    AttributeKey<String> ML_OPERATION_STATUS = AttributeKey.stringKey("pulsar.managed_ledger.operation.status");
    enum ManagedLedgerOperationStatus {
        SUCCESS,
        FAILURE;
        public final Attributes attributes = Attributes.of(ML_OPERATION_STATUS, name().toLowerCase());
    };

    /**
     * The type of the pool arena.
     */
    AttributeKey<String> ML_POOL_ARENA_TYPE = AttributeKey.stringKey("pulsar.managed_ledger.pool.arena.type");
    enum PoolArenaType {
        SMALL,
        NORMAL,
        HUGE;
        public final Attributes attributes = Attributes.of(ML_POOL_ARENA_TYPE, name().toLowerCase());
    }

    /**
     * The type of the pool chunk allocation.
     */
    AttributeKey<String> ML_POOL_CHUNK_ALLOCATION_TYPE =
            AttributeKey.stringKey("pulsar.managed_ledger.pool.chunk.allocation.type");
    enum PoolChunkAllocationType {
        ALLOCATED,
        USED;
        public final Attributes attributes = Attributes.of(ML_POOL_CHUNK_ALLOCATION_TYPE, name().toLowerCase());
    }

    /**
     * The status of the cache entry.
     */
    AttributeKey<String> ML_CACHE_ENTRY_STATUS = AttributeKey.stringKey("pulsar.managed_ledger.cache.entry.status");
    enum CacheEntryStatus {
        ACTIVE,
        EVICTED,
        INSERTED;
        public final Attributes attributes = Attributes.of(ML_CACHE_ENTRY_STATUS, name().toLowerCase());
    }

    /**
     * The result of the cache operation.
     */
    AttributeKey<String> ML_CACHE_OPERATION_STATUS =
            AttributeKey.stringKey("pulsar.managed_ledger.cache.operation.status");
    enum CacheOperationStatus {
        HIT,
        MISS;
        public final Attributes attributes = Attributes.of(ML_CACHE_OPERATION_STATUS, name().toLowerCase());
    }
}
