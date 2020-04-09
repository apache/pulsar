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

package org.apache.pulsar.common.policies.data;

/**
 * Namespace authorization operations.
 */
public enum NamespaceOperation {
    CLEAR_BACKLOG,
    DELETE_BUNDLE,
    GET_ANTI_AFFINITY_GROUP,
    GET_BACKLOG_QUOTAS,
    GET_BUNDLES,
    GET_COMPACTION_THRESHOLD,
    GET_DELAYED_DELIVERY,
    GET_DISPATCH_RATE,
    GET_MAX_CONSUMERS_PER_SUBSCRIPTION,
    GET_MAX_CONSUMERS_PER_TOPIC,
    GET_MAX_PRODUCERS_PER_TOPIC,
    GET_MAX_UNACKED_MESSAGES_PER_CONSUMER,
    GET_MAX_UNACKED_MESSAGES_PER_SUBSCRIPTION,
    GET_MESSAGE_TTL,
    GET_OFFLOAD_DELETION_LAG,
    GET_OFFLOAD_POLICIES,
    GET_OFFLOAD_THRESHOLD,
    GET_PERMISSIONS,
    GET_PERSISTENCE,
    GET_POLICIES,
    GET_PUBLISH_RATE,
    GET_REPLICATION_CLUSTERS,
    GET_REPLICATOR_DISPATCH_RATE,
    GET_RETENTION,
    GET_SCHEMA_AUTOUPDATE_STRATEGY,
    GET_SCHEMA_COMPATIBILITY_STRATEGY,
    GET_SCHEMA_VALIDATION_ENFORCED,
    GET_SUBSCRIBE_RATE,
    GET_SUBSCRIPTION_DISPATCH_RATE,
    GRANT_PERMISSIONS,
    GRANT_SUBSCRIPTION_PERMISSION,
    LIST_TOPICS,
    REVOKE_PERMISSIONS,
    REVOKE_SUBSCRIPTION_PERMISSION,
    SCHEMA_AUTOUPDATE,
    SET_BACKLOG_QUOTA,
    SET_ANTI_AFFINITY_GROUP,
    SET_COMPACTION_THRESHOLD,
    SET_DEDUPLICATION,
    SET_DELAYED_DELIVERY,
    SET_ENCRYPTION_REQUIRED,
    SET_MAX_CONSUMERS_PER_SUBSCRIPTION,
    SET_MAX_CONSUMERS_PER_TOPIC,
    SET_MAX_PRODUCERS_PER_TOPIC,
    SET_MAX_UNACKED_MESSAGES_PER_CONSUMER,
    SET_MAX_UNACKED_MESSAGES_PER_SUBSCRIPTION,
    SET_MESSAGE_TTL,
    SET_OFFLOAD_DELETION_LAG,
    SET_OFFLOAD_POLICIES,
    SET_OFFLOAD_THRESHOLD,
    SET_PERSISTENCE,
    SET_REPLICATION_CLUSTERS,
    SET_REPLICATOR_DISPATCH_RATE,
    SET_RETENTION,
    SET_SCHEMA_AUTOUPDATE_STRATEGY,
    SET_SCHEMA_COMPATIBILITY_STRATEGY,
    SET_SCHEMA_VALIDATION_ENFORCED,
    SET_SUBSCRIBE_RATE,
    SET_SUBSCRIPTION_AUTH_MODE,
    SET_SUBSCRIPTION_DISPATCH_RATE,
    UNSUBSCRIBE,
}
