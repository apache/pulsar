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
 * Cluster authorization operations.
 */
public enum ClusterOperation {
    CREATE,
    CREATE_FAILURE_DOMAIN,
    DELETE,
    DELETE_FAILURE_DOMAIN,
    GET,
    GET_BOOKIE_AFFINITY_GROUP,
    GET_FAILURE_DOMAIN,
    GET_NAMESPACE_ISOLATION_POLICIES,
    GET_PEER_CLUSTERS,
    GET_PUBLISH_RATE,
    LIST,
    LIST_FAILURE_DOMAIN,
    SET_BOOKIE_AFFINITY_GROUP,
    SET_DELAYED_DELIVERY,
    SET_FAILURE_DOMAIN,
    SET_NAMESPACE_ISOLATION_POLICIES,
    SET_PUBLISH_RATE,
    SET_REPLICATOR_DISPATCH_RATE,
    SET_SUBSCRIBE_RATE,
    SET_SUBSCRIPTION_DISPATCH_RATE,
    SET_TOPIC_DISPATCH_RATE,
    SPLIT_NAMESPACE_BUNDLE,
    UNLOAD_NAMESPACE,
    UPDATE,
    UPDATE_FAILURE_DOMAIN,
    UPDATE_PEER_CLUSTERS,
}
