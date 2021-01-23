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
package org.apache.pulsar.client.admin;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;

/**
 * @deprecated since 2.0. See {@link Topics}
 */
@Deprecated
public interface NonPersistentTopics {



    /**
     * Get metadata of a partitioned topic.
     * <p/>
     * Get metadata of a partitioned topic.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @return Partitioned topic metadata
     * @throws PulsarAdminException
     */
    PartitionedTopicMetadata getPartitionedTopicMetadata(String topic) throws PulsarAdminException;

    /**
     * Get metadata of a partitioned topic asynchronously.
     * <p/>
     * Get metadata of a partitioned topic asynchronously.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @return a future that can be used to track when the partitioned topic metadata is returned
     */
    CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(String topic);

    /**
     * Get the stats for the topic.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>
     * {
     *   "msgRateIn" : 100.0,                    // Total rate of messages published on the topic. msg/s
     *   "msgThroughputIn" : 10240.0,            // Total throughput of messages published on the topic. byte/s
     *   "msgRateOut" : 100.0,                   // Total rate of messages delivered on the topic. msg/s
     *   "msgThroughputOut" : 10240.0,           // Total throughput of messages delivered on the topic. byte/s
     *   "averageMsgSize" : 1024.0,              // Average size of published messages. bytes
     *   "publishers" : [                        // List of publishes on this topic with their stats
     *      {
     *          "producerId" : 10                // producer id
     *          "address"   : 10.4.1.23:3425     // IP and port for this producer
     *          "connectedSince" : 2014-11-21 23:54:46 // Timestamp of this published connection
     *          "msgRateIn" : 100.0,             // Total rate of messages published by this producer. msg/s
     *          "msgThroughputIn" : 10240.0,     // Total throughput of messages published by this producer. byte/s
     *          "averageMsgSize" : 1024.0,       // Average size of published messages by this producer. bytes
     *      },
     *   ],
     *   "subscriptions" : {                     // Map of subscriptions on this topic
     *     "sub1" : {
     *       "msgRateOut" : 100.0,               // Total rate of messages delivered on this subscription. msg/s
     *       "msgThroughputOut" : 10240.0,       // Total throughput delivered on this subscription. bytes/s
     *       "msgBacklog" : 0,                   // Number of messages in the subscriotion backlog
     *       "type" : Exclusive                  // Whether the subscription is exclusive or shared
     *       "consumers" [                       // List of consumers on this subscription
     *          {
     *              "id" : 5                            // Consumer id
     *              "address" : 10.4.1.23:3425          // IP and port for this consumer
     *              "connectedSince" : 2014-11-21 23:54:46 // Timestamp of this consumer connection
     *              "msgRateOut" : 100.0,               // Total rate of messages delivered to this consumer. msg/s
     *              "msgThroughputOut" : 10240.0,       // Total throughput delivered to this consumer. bytes/s
     *          }
     *       ],
     *   },
     *   "replication" : {                    // Replication statistics
     *     "cluster_1" : {                    // Cluster name in the context of from-cluster or to-cluster
     *       "msgRateIn" : 100.0,             // Total rate of messages received from this remote cluster. msg/s
     *       "msgThroughputIn" : 10240.0,     // Total throughput received from this remote cluster. bytes/s
     *       "msgRateOut" : 100.0,            // Total rate of messages delivered to the replication-subscriber. msg/s
     *       "msgThroughputOut" : 10240.0,    // Total throughput delivered to the replication-subscriber. bytes/s
     *       "connected" : true,              // Whether the replication-subscriber is currently connected locally
     *     },
     *     "cluster_2" : {
     *       "msgRateIn" : 100.0,
     *       "msgThroughputIn" : 10240.0,
     *       "msgRateOut" : 100.0,
     *       "msghroughputOut" : 10240.0,
     *       "connected" : true,
     *     }
     *   },
     * }
     * </code>
     * </pre>
     * <p/>
     * All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.
     *
     * @param topic
     *            Topic name
     * @return the topic statistics
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    NonPersistentTopicStats getStats(String topic) throws PulsarAdminException;

    /**
     * Get the stats for the topic asynchronously. All the rates are computed over a 1 minute window and are relative
     * the last completed 1 minute period.
     *
     * @param topic
     *            Topic name
     *
     * @return a future that can be used to track when the topic statistics are returned
     *
     */
    CompletableFuture<NonPersistentTopicStats> getStatsAsync(String topic);

    /**
     * Get the internal stats for the topic.
     * <p/>
     * Access the internal state of the topic
     *
     * @param topic
     *            Topic name
     * @return the topic statistics
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    PersistentTopicInternalStats getInternalStats(String topic) throws PulsarAdminException;

    /**
     * Get the internal stats for the topic asynchronously.
     *
     * @param topic
     *            Topic Name
     *
     * @return a future that can be used to track when the internal topic statistics are returned
     */
    CompletableFuture<PersistentTopicInternalStats> getInternalStatsAsync(String topic);

    /**
     * Create a partitioned topic.
     * <p/>
     * Create a partitioned topic. It needs to be called before creating a producer for a partitioned topic.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @param numPartitions
     *            Number of partitions to create of the topic
     * @throws PulsarAdminException
     */
    void createPartitionedTopic(String topic, int numPartitions) throws PulsarAdminException;

    /**
     * Create a partitioned topic asynchronously.
     * <p/>
     * Create a partitioned topic asynchronously. It needs to be called before creating a producer for a partitioned
     * topic.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @param numPartitions
     *            Number of partitions to create of the topic
     * @return a future that can be used to track when the partitioned topic is created
     */
    CompletableFuture<Void> createPartitionedTopicAsync(String topic, int numPartitions);

    /**
     * Unload a topic.
     * <p/>
     *
     * @param topic
     *            Topic name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void unload(String topic) throws PulsarAdminException;

    /**
     * Unload a topic asynchronously.
     * <p/>
     *
     * @param topic
     *            Topic name
     *
     * @return a future that can be used to track when the topic is unloaded
     */
    CompletableFuture<Void> unloadAsync(String topic);

    /**
     * Get list of topics exist into given bundle.
     *
     * @param namespace
     * @param bundleRange
     * @return
     * @throws PulsarAdminException
     */
    List<String> getListInBundle(String namespace, String bundleRange)
            throws PulsarAdminException;

    /**
     * Get list of topics exist into given bundle asynchronously.
     *
     * @param namespace
     * @param bundleRange
     * @return
     */
    CompletableFuture<List<String>> getListInBundleAsync(String namespace, String bundleRange);

    /**
     * Get list of topics exist into given namespace.
     *
     * @param namespace
     * @return
     * @throws PulsarAdminException
     */
    List<String> getList(String namespace) throws PulsarAdminException;

    /**
     * Get list of topics exist into given namespace asynchronously.
     *
     * @param namespace
     * @return
     */
    CompletableFuture<List<String>> getListAsync(String namespace);

}
