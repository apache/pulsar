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

import java.util.List;
import java.util.Map;

/**
 * Statistics for a non-persistent topic.
 */
public interface NonPersistentTopicStats extends TopicStats {

    /**
     * for non-persistent topic: broker drops msg if publisher publishes messages more than configured max inflight
     * messages per connection.
     **/
    double getMsgDropRate();

    /** List of connected publishers on this topic w/ their stats. */
    List<? extends NonPersistentPublisherStats> getPublishers();

    /** Map of subscriptions with their individual statistics. */
    Map<String, ? extends NonPersistentSubscriptionStats> getSubscriptions();

    /** Map of replication statistics by remote cluster context. */
    Map<String, ? extends NonPersistentReplicatorStats> getReplication();
}
