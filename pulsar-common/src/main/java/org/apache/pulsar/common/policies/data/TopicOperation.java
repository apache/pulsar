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
 * Topic authorization operations.
 */
public enum TopicOperation {
    BUNDLE_RANGE,
    COMPACT,
    COMPACTION_STATUS,
    CONSUME,
    CREATE,
    CREATE_PARTITIONED_TOPIC,
    CREATE_SUBSCRIPTION,
    DELETE,
    DELETE_PARTITIONED_TOPIC,
    EXPIRE_MESSAGES,
    EXPIRE_MESSAGES_ALL_SUBSCRIPTIONS,
    GET_PARTITIONED_TOPIC_METADATA,
    GRANT_PERMISSION,
    INFO_INTERNAL,
    LAST_MESSAGE_ID,
    LIST,
    LOOKUP,
    OFFLOAD,
    OFFLOAD_STATUS,
    PARTITIONED_STATS,
    PEEK_MESSAGES,
    PERMISSIONS,
    PRODUCE,
    RESET_CURSOR,
    REVOKE_PERMISSION,
    SKIP,
    SKIP_ALL,
    STATS,
    STATS_INTERNAL,
    SUBSCRIPTIONS,
    TERMINATE,
    UNLOAD,
    UNSUBSCRIBE,
    UPDATE_PARTITIONED_TOPIC,
}
