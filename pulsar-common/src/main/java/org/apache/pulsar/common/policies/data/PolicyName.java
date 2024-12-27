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
package org.apache.pulsar.common.policies.data;

/**
 * PolicyName authorization operations.
 */
public enum PolicyName {
    ALL,
    ANTI_AFFINITY,
    AUTO_SUBSCRIPTION_CREATION,
    AUTO_TOPIC_CREATION,
    BACKLOG,
    COMPACTION,
    DELAYED_DELIVERY,
    INACTIVE_TOPIC,
    DEDUPLICATION,
    MAX_CONSUMERS,
    MAX_PRODUCERS,
    DEDUPLICATION_SNAPSHOT,
    MAX_UNACKED,
    MAX_SUBSCRIPTIONS,
    OFFLOAD,
    PARTITION,
    PERSISTENCE,
    RATE,
    RETENTION,
    REPLICATION,
    REPLICATION_RATE,
    SCHEMA_COMPATIBILITY_STRATEGY,
    SUBSCRIPTION_AUTH_MODE,
    SUBSCRIPTION_EXPIRATION_TIME,
    ENCRYPTION,
    TTL,
    MAX_TOPICS,
    RESOURCEGROUP,
    ENTRY_FILTERS,
    SHADOW_TOPIC,
    ALLOW_CLUSTERS,
    REPLICATED_SUBSCRIPTION,
}
