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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAllowedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TopicStats;
/**
 * Admin interface for Topics management.
 */
public interface Topics {

    /**
     * Get the both persistent and non-persistent topics under a namespace.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>["topic://my-tenant/my-namespace/topic-1",
     *  "topic://my-tenant/my-namespace/topic-2"]</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @return a list of topics
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> getList(String namespace) throws PulsarAdminException;

    /**
     * Get the list of topics under a namespace.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>["topic://my-tenant/my-namespace/topic-1",
     *  "topic://my-tenant/my-namespace/topic-2"]</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @param topicDomain
     *            use {@link TopicDomain#persistent} to get persistent topics
     *            use {@link TopicDomain#non_persistent} to get non-persistent topics
     *            Use null to get both persistent and non-persistent topics
     *
     * @return a list of topics
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> getList(String namespace, TopicDomain topicDomain) throws PulsarAdminException;

    /**
     * Get both persistent and non-persistent topics under a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>["topic://my-tenant/my-namespace/topic-1",
     *  "topic://my-tenant/my-namespace/topic-2"]</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @return a list of topics
     */
    CompletableFuture<List<String>> getListAsync(String namespace);

    /**
     * Get the list of topics under a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>["topic://my-tenant/my-namespace/topic-1",
     *  "topic://my-tenant/my-namespace/topic-2"]</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     *
     * @param topicDomain
     *            use {@link TopicDomain#persistent} to get persistent topics
     *            use {@link TopicDomain#non_persistent} to get non-persistent topics
     *            Use null to get both persistent and non-persistent topics
     *
     * @return a list of topics
     */
    CompletableFuture<List<String>> getListAsync(String namespace, TopicDomain topicDomain);

    /**
     * Get the list of partitioned topics under a namespace.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>["persistent://my-tenant/my-namespace/topic-1",
     *  "persistent://my-tenant/my-namespace/topic-2"]</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @return a list of partitioned topics
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> getPartitionedTopicList(String namespace) throws PulsarAdminException;

    /**
     * Get the list of partitioned topics under a namespace asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>["persistent://my-tenant/my-namespace/topic-1",
     *  "persistent://my-tenant/my-namespace/topic-2"]</code>
     * </pre>
     *
     * @param namespace
     *            Namespace name
     * @return a list of partitioned topics
     */
    CompletableFuture<List<String>> getPartitionedTopicListAsync(String namespace);

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
     * Get permissions on a topic.
     * <p/>
     * Retrieve the effective permissions for a topic. These permissions are defined by the permissions set at the
     * namespace level combined (union) with any eventual specific permission set on the topic.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>{
     *   "role-1" : [ "produce" ],
     *   "role-2" : [ "consume" ]
     * }</code>
     * </pre>
     *
     * @param topic
     *            Topic url
     * @return a map of topics an their permissions set
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Map<String, Set<AuthAction>> getPermissions(String topic) throws PulsarAdminException;

    /**
     * Get permissions on a topic asynchronously.
     * <p/>
     * Retrieve the effective permissions for a topic. These permissions are defined by the permissions set at the
     * namespace level combined (union) with any eventual specific permission set on the topic.
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>{
     *   "role-1" : [ "produce" ],
     *   "role-2" : [ "consume" ]
     * }</code>
     * </pre>
     *
     * @param topic
     *            Topic url
     * @return a map of topics an their permissions set
     */
    CompletableFuture<Map<String, Set<AuthAction>>> getPermissionsAsync(String topic);

    /**
     * Grant permission on a topic.
     * <p/>
     * Grant a new permission to a client role on a single topic.
     * <p/>
     * Request parameter example:
     *
     * <pre>
     * <code>["produce", "consume"]</code>
     * </pre>
     *
     * @param topic
     *            Topic url
     * @param role
     *            Client role to which grant permission
     * @param actions
     *            Auth actions (produce and consume)
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws ConflictException
     *             Concurrent modification
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void grantPermission(String topic, String role, Set<AuthAction> actions) throws PulsarAdminException;

    /**
     * Grant permission on a topic asynchronously.
     * <p/>
     * Grant a new permission to a client role on a single topic.
     * <p/>
     * Request parameter example:
     *
     * <pre>
     * <code>["produce", "consume"]</code>
     * </pre>
     *
     * @param topic
     *            Topic url
     * @param role
     *            Client role to which grant permission
     * @param actions
     *            Auth actions (produce and consume)
     */
    CompletableFuture<Void> grantPermissionAsync(String topic, String role, Set<AuthAction> actions);

    /**
     * Revoke permissions on a topic.
     * <p/>
     * Revoke permissions to a client role on a single topic. If the permission was not set at the topic level, but
     * rather at the namespace level, this operation will return an error (HTTP status code 412).
     *
     * @param topic
     *            Topic url
     * @param role
     *            Client role to which remove permission
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PreconditionFailedException
     *             Permissions are not set at the topic level
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void revokePermissions(String topic, String role) throws PulsarAdminException;

    /**
     * Revoke permissions on a topic asynchronously.
     * <p/>
     * Revoke permissions to a client role on a single topic. If the permission was not set at the topic level, but
     * rather at the namespace level, this operation will return an error (HTTP status code 412).
     *
     * @param topic
     *            Topic url
     * @param role
     *            Client role to which remove permission
     */
    CompletableFuture<Void> revokePermissionsAsync(String topic, String role);

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
     * Create a non-partitioned topic.
     * <p/>
     * Create a non-partitioned topic.
     * <p/>
     *
     * @param topic Topic name
     * @throws PulsarAdminException
     */
    void createNonPartitionedTopic(String topic) throws PulsarAdminException;

    /**
     * Create a non-partitioned topic asynchronously.
     *
     * @param topic Topic name
     */
    CompletableFuture<Void> createNonPartitionedTopicAsync(String topic);

    /**
     * Create missed partitions for partitioned topic.
     * <p/>
     * When disable topic auto creation, use this method to try create missed partitions while
     * partitions create failed or users already have partitioned topic without partitions.
     *
     * @param topic partitioned topic name
     */
    void createMissedPartitions(String topic) throws PulsarAdminException;

    /**
     * Create missed partitions for partitioned topic asynchronously.
     * <p/>
     * When disable topic auto creation, use this method to try create missed partitions while
     * partitions create failed or users already have partitioned topic without partitions.
     *
     * @param topic partitioned topic name
     */
    CompletableFuture<Void> createMissedPartitionsAsync(String topic);

    /**
     * Update number of partitions of a non-global partitioned topic.
     * <p/>
     * It requires partitioned-topic to be already exist and number of new partitions must be greater than existing
     * number of partitions. Decrementing number of partitions requires deletion of topic which is not supported.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @param numPartitions
     *            Number of new partitions of already exist partitioned-topic
     *
     * @returns a future that can be used to track when the partitioned topic is updated.
     */
    void updatePartitionedTopic(String topic, int numPartitions) throws PulsarAdminException;

    /**
     * Update number of partitions of a non-global partitioned topic asynchronously.
     * <p/>
     * It requires partitioned-topic to be already exist and number of new partitions must be greater than existing
     * number of partitions. Decrementing number of partitions requires deletion of topic which is not supported.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @param numPartitions
     *            Number of new partitions of already exist partitioned-topic
     *
     * @return a future that can be used to track when the partitioned topic is updated
     */
    CompletableFuture<Void> updatePartitionedTopicAsync(String topic, int numPartitions);

    /**
     * Update number of partitions of a non-global partitioned topic.
     * <p/>
     * It requires partitioned-topic to be already exist and number of new partitions must be greater than existing
     * number of partitions. Decrementing number of partitions requires deletion of topic which is not supported.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @param numPartitions
     *            Number of new partitions of already exist partitioned-topic
     * @param updateLocalTopicOnly
     *            Used by broker for global topic with multiple replicated clusters
     * @param force
     *            Update forcefully without validating existing partitioned topic
     * @returns a future that can be used to track when the partitioned topic is updated
     */
    void updatePartitionedTopic(String topic, int numPartitions, boolean updateLocalTopicOnly, boolean force)
            throws PulsarAdminException;

    /**
     * Update number of partitions of a non-global partitioned topic asynchronously.
     * <p/>
     * It requires partitioned-topic to be already exist and number of new partitions must be greater than existing
     * number of partitions. Decrementing number of partitions requires deletion of topic which is not supported.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @param numPartitions
     *            Number of new partitions of already exist partitioned-topic
     * @param updateLocalTopicOnly
     *            Used by broker for global topic with multiple replicated clusters
     * @param force
     *            Update forcefully without validating existing partitioned topic
     * @return a future that can be used to track when the partitioned topic is updated
     */
    CompletableFuture<Void> updatePartitionedTopicAsync(String topic, int numPartitions, boolean updateLocalTopicOnly,
            boolean force);

    /**
     * Update number of partitions of a non-global partitioned topic.
     * <p/>
     * It requires partitioned-topic to be already exist and number of new partitions must be greater than existing
     * number of partitions. Decrementing number of partitions requires deletion of topic which is not supported.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @param numPartitions
     *            Number of new partitions of already exist partitioned-topic
     * @param updateLocalTopicOnly
     *            Used by broker for global topic with multiple replicated clusters
     * @returns a future that can be used to track when the partitioned topic is updated
     */
    void updatePartitionedTopic(String topic, int numPartitions, boolean updateLocalTopicOnly)
            throws PulsarAdminException;

    /**
     * Update number of partitions of a non-global partitioned topic asynchronously.
     * <p/>
     * It requires partitioned-topic to be already exist and number of new partitions must be greater than existing
     * number of partitions. Decrementing number of partitions requires deletion of topic which is not supported.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @param numPartitions
     *            Number of new partitions of already exist partitioned-topic
     * @param updateLocalTopicOnly
     *            Used by broker for global topic with multiple replicated clusters
     * @return a future that can be used to track when the partitioned topic is updated
     */
    CompletableFuture<Void> updatePartitionedTopicAsync(String topic, int numPartitions, boolean updateLocalTopicOnly);

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
     * Delete a partitioned topic.
     * <p/>
     * It will also delete all the partitions of the topic if it exists.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @param force
     *            Delete topic forcefully
     * @param deleteSchema
     *            Delete topic's schema storage
     *
     * @throws PulsarAdminException
     */
    void deletePartitionedTopic(String topic, boolean force, boolean deleteSchema) throws PulsarAdminException;

    /**
     * @see Topics#deletePartitionedTopic(String, boolean, boolean)
     */
    default void deletePartitionedTopic(String topic, boolean force) throws PulsarAdminException {
        deletePartitionedTopic(topic, force, false);
    }

    /**
     * Delete a partitioned topic asynchronously.
     * <p/>
     * It will also delete all the partitions of the topic if it exists.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @param force
     *            Delete topic forcefully
     * @param deleteSchema
     *            Delete topic's schema storage
     *
     * @return a future that can be used to track when the partitioned topic is deleted
     */
    CompletableFuture<Void> deletePartitionedTopicAsync(String topic, boolean force, boolean deleteSchema);

    /**
     * @see Topics#deletePartitionedTopic(String, boolean, boolean)
     */
    default CompletableFuture<Void> deletePartitionedTopicAsync(String topic, boolean force) {
        return deletePartitionedTopicAsync(topic, force, false);
    }

    /**
     * Delete a partitioned topic.
     * <p/>
     * It will also delete all the partitions of the topic if it exists.
     * <p/>
     *
     * @param topic
     *            Topic name
     *
     * @throws PulsarAdminException
     */
    void deletePartitionedTopic(String topic) throws PulsarAdminException;

    /**
     * Delete a partitioned topic asynchronously.
     * <p/>
     * It will also delete all the partitions of the topic if it exists.
     * <p/>
     *
     * @param topic
     *            Topic name
     */
    CompletableFuture<Void> deletePartitionedTopicAsync(String topic);

    /**
     * Delete a topic.
     * <p/>
     * Delete a topic. The topic cannot be deleted if force flag is disable and there's any active
     * subscription or producer connected to the it. Force flag deletes topic forcefully by closing
     * all active producers and consumers.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @param force
     *            Delete topic forcefully
     * @param deleteSchema
     *            Delete topic's schema storage
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PreconditionFailedException
     *             Topic has active subscriptions or producers
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void delete(String topic, boolean force, boolean deleteSchema) throws PulsarAdminException;

    /**
     * @see Topics#delete(String, boolean, boolean)
     */
    default void delete(String topic, boolean force) throws PulsarAdminException {
        delete(topic, force, false);
    }

    /**
     * Delete a topic asynchronously.
     * <p/>
     * Delete a topic asynchronously. The topic cannot be deleted if force flag is disable and there's any active
     * subscription or producer connected to the it. Force flag deletes topic forcefully by closing all active producers
     * and consumers.
     * <p/>
     *
     * @param topic
     *            topic name
     * @param force
     *            Delete topic forcefully
     * @param deleteSchema
     *            Delete topic's schema storage
     *
     * @return a future that can be used to track when the topic is deleted
     */
    CompletableFuture<Void> deleteAsync(String topic, boolean force, boolean deleteSchema);

    /**
     * @see Topics#deleteAsync(String, boolean, boolean)
     */
    default CompletableFuture<Void> deleteAsync(String topic, boolean force) {
        return deleteAsync(topic, force, false);
    }

    /**
     * Delete a topic.
     * <p/>
     * Delete a topic. The topic cannot be deleted if there's any active subscription or producer connected to the it.
     * <p/>
     *
     * @param topic
     *            Topic name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PreconditionFailedException
     *             Topic has active subscriptions or producers
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void delete(String topic) throws PulsarAdminException;

    /**
     * Delete a topic asynchronously.
     * <p/>
     * Delete a topic. The topic cannot be deleted if there's any active subscription or producer connected to the it.
     * <p/>
     *
     * @param topic
     *            Topic name
     */
    CompletableFuture<Void> deleteAsync(String topic);

    /**
     * Unload a topic.
     * <p/>
     *
     * @param topic
     *            topic name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void unload(String topic) throws PulsarAdminException;

    /**
     * Unload a topic asynchronously.
     * <p/>
     *
     * @param topic
     *            topic name
     *
     * @return a future that can be used to track when the topic is unloaded
     */
    CompletableFuture<Void> unloadAsync(String topic);

    /**
     * Terminate the topic and prevent any more messages being published on it.
     * <p/>
     *
     * @param topic
     *            topic name
     * @return the message id of the last message that was published in the topic
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    MessageId terminateTopic(String topic) throws PulsarAdminException;

    /**
     * Terminate the topic and prevent any more messages being published on it.
     * <p/>
     *
     * @param topic
     *            topic name
     * @return the message id of the last message that was published in the topic
     */
    CompletableFuture<MessageId> terminateTopicAsync(String topic);

    /**
     * Get the list of subscriptions.
     * <p/>
     * Get the list of persistent subscriptions for a given topic.
     * <p/>
     *
     * @param topic
     *            topic name
     * @return the list of subscriptions
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> getSubscriptions(String topic) throws PulsarAdminException;

    /**
     * Get the list of subscriptions asynchronously.
     * <p/>
     * Get the list of persistent subscriptions for a given topic.
     * <p/>
     *
     * @param topic
     *            topic name
     * @return a future that can be used to track when the list of subscriptions is returned
     */
    CompletableFuture<List<String>> getSubscriptionsAsync(String topic);

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
     *       "replicationBacklog" : 0,        // Number of messages pending to be replicated to this remote cluster
     *       "connected" : true,              // Whether the replication-subscriber is currently connected locally
     *     },
     *     "cluster_2" : {
     *       "msgRateIn" : 100.0,
     *       "msgThroughputIn" : 10240.0,
     *       "msgRateOut" : 100.0,
     *       "msgThroughputOut" : 10240.0,
     *       "replicationBacklog" : 0,
     *       "connected" : true,
     *     }
     *   },
     * }
     * </code>
     * </pre>
     *
     * <p>All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.
     *
     * @param topic
     *            topic name
     * @param getStatsOptions
     *            get stats options
     * @return the topic statistics
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    TopicStats getStats(String topic, GetStatsOptions getStatsOptions) throws PulsarAdminException;


    default TopicStats getStats(String topic, boolean getPreciseBacklog, boolean subscriptionBacklogSize,
                        boolean getEarliestTimeInBacklog) throws PulsarAdminException {
        GetStatsOptions getStatsOptions =
                new GetStatsOptions(getPreciseBacklog, subscriptionBacklogSize, getEarliestTimeInBacklog);
        return getStats(topic, getStatsOptions);
    }

    default TopicStats getStats(String topic, boolean getPreciseBacklog,
                        boolean subscriptionBacklogSize) throws PulsarAdminException {
        GetStatsOptions getStatsOptions = new GetStatsOptions(getPreciseBacklog, subscriptionBacklogSize, false);
        return getStats(topic, getStatsOptions);
    }

    default TopicStats getStats(String topic, boolean getPreciseBacklog) throws PulsarAdminException {
        GetStatsOptions getStatsOptions = new GetStatsOptions(getPreciseBacklog, false, false);
        return getStats(topic, getStatsOptions);
    }

    default TopicStats getStats(String topic) throws PulsarAdminException {
        return getStats(topic, new GetStatsOptions(false, false, false));
    }

    /**
     * Get the stats for the topic asynchronously. All the rates are computed over a 1 minute window and are relative
     * the last completed 1 minute period.
     *
     * @param topic
     *            topic name
     * @param getPreciseBacklog
     *            Set to true to get precise backlog, Otherwise get imprecise backlog.
     * @param subscriptionBacklogSize
     *            Whether to get backlog size for each subscription.
     * @param getEarliestTimeInBacklog
     *            Whether to get the earliest time in backlog.
     * @return a future that can be used to track when the topic statistics are returned
     *
     */
    CompletableFuture<TopicStats> getStatsAsync(String topic, boolean getPreciseBacklog,
                                                boolean subscriptionBacklogSize, boolean getEarliestTimeInBacklog);

    default CompletableFuture<TopicStats> getStatsAsync(String topic) {
        return getStatsAsync(topic, false, false, false);
    }

    /**
     * Get the internal stats for the topic.
     * <p/>
     * Access the internal state of the topic
     *
     * @param topic
     *            topic name
     * @param metadata
     *            flag to include ledger metadata
     * @return the topic statistics
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    PersistentTopicInternalStats getInternalStats(String topic, boolean metadata) throws PulsarAdminException;

    /**
     * Get the internal stats for the topic.
     * <p/>
     * Access the internal state of the topic
     *
     * @param topic
     *            topic name
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
     *            topic Name
     * @param metadata
     *            flag to include ledger metadata
     * @return a future that can be used to track when the internal topic statistics are returned
     */
    CompletableFuture<PersistentTopicInternalStats> getInternalStatsAsync(String topic, boolean metadata);

    /**
     * Get the internal stats for the topic asynchronously.
     *
     * @param topic
     *            topic Name
     * @return a future that can be used to track when the internal topic statistics are returned
     */
    CompletableFuture<PersistentTopicInternalStats> getInternalStatsAsync(String topic);

    /**
     * Get a JSON representation of the topic metadata stored in ZooKeeper.
     *
     * @param topic
     *            topic name
     * @return the topic internal metadata
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    String getInternalInfo(String topic) throws PulsarAdminException;

    /**
     * Get a JSON representation of the topic metadata stored in ZooKeeper.
     *
     * @param topic
     *            topic name
     * @return a future to receive the topic internal metadata
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    CompletableFuture<String> getInternalInfoAsync(String topic);

    /**
     * Get the stats for the partitioned topic
     * <p/>
     * Response Example:
     *
     * <pre>
     * <code>
     * {
     *   "msgRateIn" : 100.0,                 // Total rate of messages published on the partitioned topic. msg/s
     *   "msgThroughputIn" : 10240.0,         // Total throughput of messages published on the partitioned topic. byte/s
     *   "msgRateOut" : 100.0,                // Total rate of messages delivered on the partitioned topic. msg/s
     *   "msgThroughputOut" : 10240.0,        // Total throughput of messages delivered on the partitioned topic. byte/s
     *   "averageMsgSize" : 1024.0,           // Average size of published messages. bytes
     *   "publishers" : [                     // List of publishes on this partitioned topic with their stats
     *      {
     *          "msgRateIn" : 100.0,          // Total rate of messages published by this producer. msg/s
     *          "msgThroughputIn" : 10240.0,  // Total throughput of messages published by this producer. byte/s
     *          "averageMsgSize" : 1024.0,    // Average size of published messages by this producer. bytes
     *      },
     *   ],
     *   "subscriptions" : {                  // Map of subscriptions on this topic
     *     "sub1" : {
     *       "msgRateOut" : 100.0,            // Total rate of messages delivered on this subscription. msg/s
     *       "msgThroughputOut" : 10240.0,    // Total throughput delivered on this subscription. bytes/s
     *       "msgBacklog" : 0,                // Number of messages in the subscriotion backlog
     *       "type" : Exclusive               // Whether the subscription is exclusive or shared
     *       "consumers" [                    // List of consumers on this subscription
     *          {
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
     *       "replicationBacklog" : 0,        // Number of messages pending to be replicated to this remote cluster
     *       "connected" : true,              // Whether the replication-subscriber is currently connected locally
     *     },
     *     "cluster_2" : {
     *       "msgRateIn" : 100.0,
     *       "msgThroughputIn" : 10240.0,
     *       "msgRateOut" : 100.0,
     *       "msghroughputOut" : 10240.0,
     *       "replicationBacklog" : 0,
     *       "connected" : true,
     *     }
     *   },
     * }
     * </code>
     * </pre>
     *
     * <p>All the rates are computed over a 1 minute window and are relative the last completed 1 minute period.
     *
     * @param topic
     *            topic name
     * @param perPartition
     *            flag to get stats per partition
     * @param getPreciseBacklog
     *            Set to true to get precise backlog, Otherwise get imprecise backlog.
     * @param subscriptionBacklogSize
     *            Whether to get backlog size for each subscription.
     * @return the partitioned topic statistics
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     *
     */
    PartitionedTopicStats getPartitionedStats(String topic, boolean perPartition, boolean getPreciseBacklog,
                                              boolean subscriptionBacklogSize)
            throws PulsarAdminException;

    default PartitionedTopicStats getPartitionedStats(String topic, boolean perPartition) throws PulsarAdminException {
        return getPartitionedStats(topic, perPartition, false, false);
    }

    /**
     * Get the stats for the partitioned topic asynchronously.
     *
     * @param topic
     *            topic Name
     * @param perPartition
     *            flag to get stats per partition
     * @param getPreciseBacklog
     *            Set to true to get precise backlog, Otherwise get imprecise backlog.
     * @param subscriptionBacklogSize
     *            Whether to get backlog size for each subscription.
     * @return a future that can be used to track when the partitioned topic statistics are returned
     */
    CompletableFuture<PartitionedTopicStats> getPartitionedStatsAsync(
            String topic, boolean perPartition, boolean getPreciseBacklog, boolean subscriptionBacklogSize);

    default CompletableFuture<PartitionedTopicStats> getPartitionedStatsAsync(String topic, boolean perPartition) {
        return getPartitionedStatsAsync(topic, perPartition, false, false);
    }

    /**
     * Get the stats for the partitioned topic.
     *
     * @param topic
     *            topic name
     * @return
     * @throws PulsarAdminException
     */
    PartitionedTopicInternalStats getPartitionedInternalStats(String topic)
            throws PulsarAdminException;

    /**
     * Get the stats-internal for the partitioned topic asynchronously.
     *
     * @param topic
     *            topic Name
     * @return a future that can be used to track when the partitioned topic statistics are returned
     */
    CompletableFuture<PartitionedTopicInternalStats> getPartitionedInternalStatsAsync(String topic);

    /**
     * Delete a subscription.
     * <p/>
     * Delete a persistent subscription from a topic. There should not be any active consumers on the subscription.
     * <p/>
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic or subscription does not exist
     * @throws PreconditionFailedException
     *             Subscription has active consumers
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void deleteSubscription(String topic, String subName) throws PulsarAdminException;

    /**
     * Delete a subscription.
     * <p/>
     * Delete a persistent subscription from a topic. There should not be any active consumers on the subscription.
     * Force flag deletes subscription forcefully by closing all active consumers.
     * <p/>
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     * @param force
     *            Delete topic forcefully
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic or subscription does not exist
     * @throws PreconditionFailedException
     *             Subscription has active consumers
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void deleteSubscription(String topic, String subName, boolean force) throws PulsarAdminException;

    /**
     * Delete a subscription asynchronously.
     * <p/>
     * Delete a persistent subscription from a topic. There should not be any active consumers on the subscription.
     * <p/>
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     *
     * @return a future that can be used to track when the subscription is deleted
     */
    CompletableFuture<Void> deleteSubscriptionAsync(String topic, String subName);

    /**
     * Delete a subscription asynchronously.
     * <p/>
     * Delete a persistent subscription from a topic. There should not be any active consumers on the subscription.
     * Force flag deletes subscription forcefully by closing all active consumers.
     * <p/>
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     * @param force
     *            Delete topic forcefully
     *
     * @return a future that can be used to track when the subscription is deleted
     */
    CompletableFuture<Void> deleteSubscriptionAsync(String topic, String subName, boolean force);

    /**
     * Skip all messages on a topic subscription.
     * <p/>
     * Completely clears the backlog on the subscription.
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic or subscription does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void skipAllMessages(String topic, String subName) throws PulsarAdminException;

    /**
     * Skip all messages on a topic subscription asynchronously.
     * <p/>
     * Completely clears the backlog on the subscription.
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     *
     * @return a future that can be used to track when all the messages are skipped
     */
    CompletableFuture<Void> skipAllMessagesAsync(String topic, String subName);

    /**
     * Skip messages on a topic subscription.
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     * @param numMessages
     *            Number of messages
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic or subscription does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void skipMessages(String topic, String subName, long numMessages) throws PulsarAdminException;

    /**
     * Skip messages on a topic subscription asynchronously.
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     * @param numMessages
     *            Number of messages
     *
     * @return a future that can be used to track when the number of messages are skipped
     */
    CompletableFuture<Void> skipMessagesAsync(String topic, String subName, long numMessages);

    /**
     * Expire all messages older than given N (expireTimeInSeconds) seconds for a given subscription.
     *
     * @param topic
     *            topic name
     * @param subscriptionName
     *            Subscription name
     * @param expireTimeInSeconds
     *            Expire messages older than time in seconds
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void expireMessages(String topic, String subscriptionName, long expireTimeInSeconds)
            throws PulsarAdminException;

    /**
     * Expire all messages older than given N (expireTimeInSeconds) seconds for a given subscription asynchronously.
     *
     * @param topic
     *            topic name
     * @param subscriptionName
     *            Subscription name
     * @param expireTimeInSeconds
     *            Expire messages older than time in seconds
     * @return
     */
    CompletableFuture<Void> expireMessagesAsync(String topic, String subscriptionName,
            long expireTimeInSeconds);

    /**
     * Expire all messages older than given N (expireTimeInSeconds) seconds for a given subscription.
     *
     * @param topic
     *            topic name
     * @param subscriptionName
     *            Subscription name
     * @param messageId
     *            Position before which all messages will be expired.
     * @param isExcluded
     *            Will message at passed in position also be expired.
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void expireMessages(String topic, String subscriptionName, MessageId messageId, boolean isExcluded)
            throws PulsarAdminException;

    /**
     * Expire all messages older than given N (expireTimeInSeconds) seconds for a given subscription asynchronously.
     *
     * @param topic
     *            topic name
     * @param subscriptionName
     *            Subscription name
     * @param messageId
     *            Position before which all messages will be expired.
     * @param isExcluded
     *            Will message at passed in position also be expired.
     * @return
     *            A {@link CompletableFuture} that'll be completed when expire message is done.
     */
    CompletableFuture<Void> expireMessagesAsync(String topic, String subscriptionName,
                                                MessageId messageId, boolean isExcluded);

    /**
     * Expire all messages older than given N seconds for all subscriptions of the persistent-topic.
     *
     * @param topic
     *            topic name
     * @param expireTimeInSeconds
     *            Expire messages older than time in seconds
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void expireMessagesForAllSubscriptions(String topic, long expireTimeInSeconds)
            throws PulsarAdminException;

    /**
     * Expire all messages older than given N seconds for all subscriptions of the persistent-topic asynchronously.
     *
     * @param topic
     *            topic name
     * @param expireTimeInSeconds
     *            Expire messages older than time in seconds
     */
    CompletableFuture<Void> expireMessagesForAllSubscriptionsAsync(String topic, long expireTimeInSeconds);

    /**
     * Peek messages from a topic subscription.
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     * @param numMessages
     *            Number of messages
     * @return
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic or subscription does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<Message<byte[]>> peekMessages(String topic, String subName, int numMessages) throws PulsarAdminException;

    /**
     * Peek messages from a topic subscription asynchronously.
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     * @param numMessages
     *            Number of messages
     * @return a future that can be used to track when the messages are returned
     */
    CompletableFuture<List<Message<byte[]>>> peekMessagesAsync(String topic, String subName, int numMessages);

    /**
     * Get a message by its messageId via a topic subscription.
     * @param topic
     *            Topic name
     * @param ledgerId
     *            Ledger id
     * @param entryId
     *            Entry id
     * @return the message indexed by the messageId
     * @throws PulsarAdminException
     *            Unexpected error
     */
    Message<byte[]> getMessageById(String topic, long ledgerId, long entryId) throws PulsarAdminException;

    /**
     * Get a message by its messageId via a topic subscription asynchronously.
     * @param topic
     *            Topic name
     * @param ledgerId
     *            Ledger id
     * @param entryId
     *            Entry id
     * @return a future that can be used to track when the message is returned
     */
    CompletableFuture<Message<byte[]>> getMessageByIdAsync(String topic, long ledgerId, long entryId);

    /**
     * Get message ID published at or just after this absolute timestamp (in ms).
     * @param topic
     *            Topic name
     * @param timestamp
     *            Timestamp
     * @return MessageId
     * @throws PulsarAdminException
     *            Unexpected error
     */
    MessageId getMessageIdByTimestamp(String topic, long timestamp) throws PulsarAdminException;

    /**
     * Get message ID published at or just after this absolute timestamp (in ms) asynchronously.
     * @param topic
     *            Topic name
     * @param timestamp
     *            Timestamp
     * @return a future that can be used to track when the message ID is returned.
     */
    CompletableFuture<MessageId> getMessageIdByTimestampAsync(String topic, long timestamp);

    /**
     * Create a new subscription on a topic.
     *
     * @param topic
     *            topic name
     * @param subscriptionName
     *            Subscription name
     * @param messageId
     *            The {@link MessageId} on where to initialize the subscription. It could be {@link MessageId#latest},
     *            {@link MessageId#earliest} or a specific message id.
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws ConflictException
     *             Subscription already exists
     * @throws NotAllowedException
     *             Command disallowed for requested resource
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createSubscription(String topic, String subscriptionName, MessageId messageId)
            throws PulsarAdminException;

    /**
     * Create a new subscription on a topic.
     *
     * @param topic
     *            topic name
     * @param subscriptionName
     *            Subscription name
     * @param messageId
     *            The {@link MessageId} on where to initialize the subscription. It could be {@link MessageId#latest},
     *            {@link MessageId#earliest} or a specific message id.
     */
    CompletableFuture<Void> createSubscriptionAsync(String topic, String subscriptionName, MessageId messageId);

    /**
     * Reset cursor position on a topic subscription.
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     * @param timestamp
     *            reset subscription to position closest to time in ms since epoch
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic or subscription does not exist
     * @throws NotAllowedException
     *             Command disallowed for requested resource
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void resetCursor(String topic, String subName, long timestamp) throws PulsarAdminException;

    /**
     * Reset cursor position on a topic subscription.
     * <p/>
     * and start consume messages from the next position of the reset position.
     * @param topic
     * @param subName
     * @param messageId
     * @param isExcluded
     * @throws PulsarAdminException
     */
    void resetCursor(String topic, String subName, MessageId messageId, boolean isExcluded) throws PulsarAdminException;

    /**
     * Reset cursor position on a topic subscription.
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     * @param timestamp
     *            reset subscription to position closest to time in ms since epoch
     */
    CompletableFuture<Void> resetCursorAsync(String topic, String subName, long timestamp);

    /**
     * Reset cursor position on a topic subscription.
     * <p/>
     * and start consume messages from the next position of the reset position.
     * @param topic
     * @param subName
     * @param messageId
     * @param isExcluded
     * @return
     */
    CompletableFuture<Void> resetCursorAsync(String topic, String subName, MessageId messageId, boolean isExcluded);

    /**
     * Reset cursor position on a topic subscription.
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     * @param messageId
     *            reset subscription to messageId (or previous nearest messageId if given messageId is not valid)
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic or subscription does not exist
     * @throws NotAllowedException
     *             Command disallowed for requested resource
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void resetCursor(String topic, String subName, MessageId messageId) throws PulsarAdminException;

    /**
     * Reset cursor position on a topic subscription.
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     * @param messageId
     *            reset subscription to messageId (or previous nearest messageId if given messageId is not valid)
     */
    CompletableFuture<Void> resetCursorAsync(String topic, String subName, MessageId messageId);

    /**
     * Trigger compaction to run for a topic. A single topic can only have one instance of compaction
     * running at any time. Any attempt to trigger another will be met with a ConflictException.
     *
     * @param topic
     *            The topic on which to trigger compaction
     */
    void triggerCompaction(String topic) throws PulsarAdminException;

    /**
     * Trigger compaction to run for a topic asynchronously.
     *
     * @param topic
     *            The topic on which to trigger compaction
     */
    CompletableFuture<Void> triggerCompactionAsync(String topic);

    /**
     * Check the status of an ongoing compaction for a topic.
     *
     * @param topic The topic whose compaction status we wish to check
     */
    LongRunningProcessStatus compactionStatus(String topic) throws PulsarAdminException;

    /**
     * Check the status of an ongoing compaction for a topic asynchronously.
     *
     * @param topic The topic whose compaction status we wish to check
     */
    CompletableFuture<LongRunningProcessStatus> compactionStatusAsync(String topic);

    /**
     * Trigger offloading messages in topic to longterm storage.
     *
     * @param topic the topic to offload
     * @param messageId ID of maximum message which should be offloaded
     */
    void triggerOffload(String topic, MessageId messageId) throws PulsarAdminException;

    /**
     * Trigger offloading messages in topic to longterm storage asynchronously.
     *
     * @param topic the topic to offload
     * @param messageId ID of maximum message which should be offloaded
     */
    CompletableFuture<Void> triggerOffloadAsync(String topic, MessageId messageId);

    /**
     * Check the status of an ongoing offloading operation for a topic.
     *
     * @param topic the topic being offloaded
     * @return the status of the offload operation
     */
    OffloadProcessStatus offloadStatus(String topic) throws PulsarAdminException;

    /**
     * Check the status of an ongoing offloading operation for a topic asynchronously.
     *
     * @param topic the topic being offloaded
     * @return the status of the offload operation
     */
    CompletableFuture<OffloadProcessStatus> offloadStatusAsync(String topic);

    /**
     * Get the last commit message Id of a topic.
     *
     * @param topic the topic name
     * @return
     * @throws PulsarAdminException
     */
    MessageId getLastMessageId(String topic) throws PulsarAdminException;

    /**
     * Get the last commit message Id of a topic asynchronously.
     *
     * @param topic the topic name
     * @return
     */
    CompletableFuture<MessageId> getLastMessageIdAsync(String topic);

    /**
     * Get backlog quota map for a topic.
     * Response example:
     *
     * <pre>
     * <code>
     *  {
     *      "namespace_memory" : {
     *          "limit" : "134217728",
     *          "policy" : "consumer_backlog_eviction"
     *      },
     *      "destination_storage" : {
     *          "limit" : "-1",
     *          "policy" : "producer_exception"
     *      }
     *  }
     * </code>
     * </pre>
     *
     * @param topic
     *            Topic name
     *
     * @throws NotAuthorizedException
     *             Permission denied
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String topic)
            throws PulsarAdminException;

    /**
     * Get applied backlog quota map for a topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String topic, boolean applied)
            throws PulsarAdminException;

    /**
     * Get backlog size by a message ID.
     * @param topic
     *            Topic name
     * @param messageId
     *            message ID
     * @return the backlog size from
     * @throws PulsarAdminException
     *            Unexpected error
     */
    Long getBacklogSizeByMessageId(String topic, MessageId messageId) throws PulsarAdminException;

    /**
     * Get backlog size by a message ID asynchronously.
     * @param topic
     *            Topic name
     * @param messageId
     *            message ID
     * @return the backlog size from
     */
    CompletableFuture<Long> getBacklogSizeByMessageIdAsync(String topic, MessageId messageId);

    /**
     * Set a backlog quota for a topic.
     * The backlog quota can be set on this resource:
     *
     * <p>
     * Request parameter example:
     *</p>
     *
     * <pre>
     * <code>
     * {
     *     "limit" : "134217728",
     *     "policy" : "consumer_backlog_eviction"
     * }
     * </code>
     * </pre>
     *
     * @param topic
     *            Topic name
     * @param backlogQuota
     *            the new BacklogQuota
     * @param backlogQuotaType
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void setBacklogQuota(String topic, BacklogQuota backlogQuota,
                         BacklogQuota.BacklogQuotaType backlogQuotaType) throws PulsarAdminException;

    @Deprecated
    default void setBacklogQuota(String topic, BacklogQuota backlogQuota) throws PulsarAdminException {
        setBacklogQuota(topic, backlogQuota, BacklogQuota.BacklogQuotaType.destination_storage);
    }

    /**
     * Remove a backlog quota policy from a topic.
     * The namespace backlog policy will fall back to the default.
     *
     * @param topic
     *            Topic name
     * @param backlogQuotaType
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void removeBacklogQuota(String topic, BacklogQuota.BacklogQuotaType backlogQuotaType) throws PulsarAdminException;

    @Deprecated
    default void removeBacklogQuota(String topic)
            throws PulsarAdminException {
        removeBacklogQuota(topic, BacklogQuota.BacklogQuotaType.destination_storage);
    }

    /**
     * Get the delayed delivery policy applied for a specified topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    DelayedDeliveryPolicies getDelayedDeliveryPolicy(String topic
            , boolean applied) throws PulsarAdminException;

    /**
     * Get the delayed delivery policy applied for a specified topic asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    @Deprecated
    CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryPolicyAsync(String topic
            , boolean applied);
    /**
     * Get the delayed delivery policy for a specified topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    DelayedDeliveryPolicies getDelayedDeliveryPolicy(String topic) throws PulsarAdminException;

    /**
     * Get the delayed delivery policy for a specified topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryPolicyAsync(String topic);

    /**
     * Set the delayed delivery policy for a specified topic.
     * @param topic
     * @param delayedDeliveryPolicies
     * @throws PulsarAdminException
     */
    @Deprecated
    void setDelayedDeliveryPolicy(String topic
            , DelayedDeliveryPolicies delayedDeliveryPolicies) throws PulsarAdminException;

    /**
     * Set the delayed delivery policy for a specified topic asynchronously.
     * @param topic
     * @param delayedDeliveryPolicies
     * @return
     */
    @Deprecated
    CompletableFuture<Void> setDelayedDeliveryPolicyAsync(String topic
            , DelayedDeliveryPolicies delayedDeliveryPolicies);

    /**
     * Remove the delayed delivery policy for a specified topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Void> removeDelayedDeliveryPolicyAsync(String topic);

    /**
     * Remove the delayed delivery policy for a specified topic.
     * @param topic
     * @throws PulsarAdminException
     */
    @Deprecated
    void removeDelayedDeliveryPolicy(String topic) throws PulsarAdminException;

    /**
     * Set message TTL for a topic.
     *
     * @param topic
     *          Topic name
     * @param messageTTLInSecond
     *          Message TTL in second.
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void setMessageTTL(String topic, int messageTTLInSecond) throws PulsarAdminException;

    /**
     * Get message TTL for a topic.
     *
     * @param topic
     * @return Message TTL in second.
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    Integer getMessageTTL(String topic) throws PulsarAdminException;

    /**
     * Get message TTL applied for a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    Integer getMessageTTL(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Remove message TTL for a topic.
     *
     * @param topic
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void removeMessageTTL(String topic) throws PulsarAdminException;

    /**
     * Set the retention configuration on a topic.
     * <p/>
     * Set the retention configuration on a topic. This operation requires Pulsar super-user access.
     * <p/>
     * Request parameter example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "retentionTimeInMinutes" : 60,            // how long to retain messages
     *     "retentionSizeInMB" : 1024,              // retention backlog limit
     * }
     * </code>
     * </pre>
     *
     * @param topic
     *            Topic name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws ConflictException
     *             Concurrent modification
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void setRetention(String topic, RetentionPolicies retention) throws PulsarAdminException;

    /**
     * Set the retention configuration for all the topics on a topic asynchronously.
     * <p/>
     * Set the retention configuration on a topic. This operation requires Pulsar super-user access.
     * <p/>
     * Request parameter example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "retentionTimeInMinutes" : 60,            // how long to retain messages
     *     "retentionSizeInMB" : 1024,              // retention backlog limit
     * }
     * </code>
     * </pre>
     *
     * @param topic
     *            Topic name
     */
    @Deprecated
    CompletableFuture<Void> setRetentionAsync(String topic, RetentionPolicies retention);

    /**
     * Get the retention configuration for a topic.
     * <p/>
     * Get the retention configuration for a topic.
     * <p/>
     * Response example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "retentionTimeInMinutes" : 60,            // how long to retain messages
     *     "retentionSizeInMB" : 1024,              // retention backlog limit
     * }
     * </code>
     * </pre>
     *
     * @param topic
     *            Topic name
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws ConflictException
     *             Concurrent modification
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    RetentionPolicies getRetention(String topic) throws PulsarAdminException;

    /**
     * Get the retention configuration for a topic asynchronously.
     * <p/>
     * Get the retention configuration for a topic.
     * <p/>
     *
     * @param topic
     *            Topic name
     */
    @Deprecated
    CompletableFuture<RetentionPolicies> getRetentionAsync(String topic);

    /**
     * Get the applied retention configuration for a topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    RetentionPolicies getRetention(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the applied retention configuration for a topic asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    @Deprecated
    CompletableFuture<RetentionPolicies> getRetentionAsync(String topic, boolean applied);

    /**
     * Remove the retention configuration for all the topics on a topic.
     * <p/>
     * Remove the retention configuration on a topic. This operation requires Pulsar super-user access.
     * <p/>
     * Request parameter example:
     * <p/>
     *
     * @param topic
     *            Topic name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws ConflictException
     *             Concurrent modification
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void removeRetention(String topic) throws PulsarAdminException;

    /**
     * Remove the retention configuration for all the topics on a topic asynchronously.
     * <p/>
     * Remove the retention configuration on a topic. This operation requires Pulsar super-user access.
     * <p/>
     * Request parameter example:
     * <p/>
     *
     * <pre>
     * <code>
     * {
     *     "retentionTimeInMinutes" : 60,            // how long to retain messages
     *     "retentionSizeInMB" : 1024,              // retention backlog limit
     * }
     * </code>
     * </pre>
     *
     * @param topic
     *            Topic name
     */
    @Deprecated
    CompletableFuture<Void> removeRetentionAsync(String topic);

    /**
     * get max unacked messages on consumer of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    Integer getMaxUnackedMessagesOnConsumer(String topic) throws PulsarAdminException;

    /**
     * get max unacked messages on consumer of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Integer> getMaxUnackedMessagesOnConsumerAsync(String topic);

    /**
     * get applied max unacked messages on consumer of a topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    Integer getMaxUnackedMessagesOnConsumer(String topic, boolean applied) throws PulsarAdminException;

    /**
     * get applied max unacked messages on consumer of a topic asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    @Deprecated
    CompletableFuture<Integer> getMaxUnackedMessagesOnConsumerAsync(String topic, boolean applied);

    /**
     * set max unacked messages on consumer of a topic.
     * @param topic
     * @param maxNum
     * @throws PulsarAdminException
     */
    @Deprecated
    void setMaxUnackedMessagesOnConsumer(String topic, int maxNum) throws PulsarAdminException;

    /**
     * set max unacked messages on consumer of a topic asynchronously.
     * @param topic
     * @param maxNum
     * @return
     */
    @Deprecated
    CompletableFuture<Void> setMaxUnackedMessagesOnConsumerAsync(String topic, int maxNum);

    /**
     * remove max unacked messages on consumer of a topic.
     * @param topic
     * @throws PulsarAdminException
     */
    @Deprecated
    void removeMaxUnackedMessagesOnConsumer(String topic) throws PulsarAdminException;

    /**
     * remove max unacked messages on consumer of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Void> removeMaxUnackedMessagesOnConsumerAsync(String topic);

    /**
     * Get inactive topic policies applied for a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    InactiveTopicPolicies getInactiveTopicPolicies(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get inactive topic policies applied for a topic asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    @Deprecated
    CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String topic, boolean applied);
    /**
     * get inactive topic policies of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    InactiveTopicPolicies getInactiveTopicPolicies(String topic) throws PulsarAdminException;

    /**
     * get inactive topic policies of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String topic);

    /**
     * set inactive topic policies of a topic.
     * @param topic
     * @param inactiveTopicPolicies
     * @throws PulsarAdminException
     */
    @Deprecated
    void setInactiveTopicPolicies(String topic
            , InactiveTopicPolicies inactiveTopicPolicies) throws PulsarAdminException;

    /**
     * set inactive topic policies of a topic asynchronously.
     * @param topic
     * @param inactiveTopicPolicies
     * @return
     */
    @Deprecated
    CompletableFuture<Void> setInactiveTopicPoliciesAsync(String topic, InactiveTopicPolicies inactiveTopicPolicies);

    /**
     * remove inactive topic policies of a topic.
     * @param topic
     * @throws PulsarAdminException
     */
    @Deprecated
    void removeInactiveTopicPolicies(String topic) throws PulsarAdminException;

    /**
     * remove inactive topic policies of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Void> removeInactiveTopicPoliciesAsync(String topic);

    /**
     * get offload policies of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    OffloadPolicies getOffloadPolicies(String topic) throws PulsarAdminException;

    /**
     * get offload policies of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String topic);

    /**
     * get applied offload policies of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    OffloadPolicies getOffloadPolicies(String topic, boolean applied) throws PulsarAdminException;

    /**
     * get applied offload policies of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String topic, boolean applied);

    /**
     * set offload policies of a topic.
     * @param topic
     * @param offloadPolicies
     * @throws PulsarAdminException
     */
    @Deprecated
    void setOffloadPolicies(String topic, OffloadPolicies offloadPolicies) throws PulsarAdminException;

    /**
     * set offload policies of a topic asynchronously.
     * @param topic
     * @param offloadPolicies
     * @return
     */
    @Deprecated
    CompletableFuture<Void> setOffloadPoliciesAsync(String topic, OffloadPolicies offloadPolicies);

    /**
     * remove offload policies of a topic.
     * @param topic
     * @throws PulsarAdminException
     */
    @Deprecated
    void removeOffloadPolicies(String topic) throws PulsarAdminException;

    /**
     * remove offload policies of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Void> removeOffloadPoliciesAsync(String topic);

    /**
     * get max unacked messages on subscription of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    Integer getMaxUnackedMessagesOnSubscription(String topic) throws PulsarAdminException;

    /**
     * get max unacked messages on subscription of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Integer> getMaxUnackedMessagesOnSubscriptionAsync(String topic);

    /**
     * get max unacked messages on subscription of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    Integer getMaxUnackedMessagesOnSubscription(String topic, boolean applied) throws PulsarAdminException;

    /**
     * get max unacked messages on subscription of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Integer> getMaxUnackedMessagesOnSubscriptionAsync(String topic, boolean applied);

    /**
     * set max unacked messages on subscription of a topic.
     * @param topic
     * @param maxNum
     * @throws PulsarAdminException
     */
    @Deprecated
    void setMaxUnackedMessagesOnSubscription(String topic, int maxNum) throws PulsarAdminException;

    /**
     * set max unacked messages on subscription of a topic asynchronously.
     * @param topic
     * @param maxNum
     * @return
     */
    @Deprecated
    CompletableFuture<Void> setMaxUnackedMessagesOnSubscriptionAsync(String topic, int maxNum);

    /**
     * remove max unacked messages on subscription of a topic.
     * @param topic
     * @throws PulsarAdminException
     */
    @Deprecated
    void removeMaxUnackedMessagesOnSubscription(String topic) throws PulsarAdminException;

    /**
     * remove max unacked messages on subscription of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Void> removeMaxUnackedMessagesOnSubscriptionAsync(String topic);

    /**
     * Set the configuration of persistence policies for specified topic.
     *
     * @param topic Topic name
     * @param persistencePolicies Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    void setPersistence(String topic, PersistencePolicies persistencePolicies) throws PulsarAdminException;

    /**
     * Set the configuration of persistence policies for specified topic asynchronously.
     *
     * @param topic Topic name
     * @param persistencePolicies Configuration of bookkeeper persistence policies
     */
    @Deprecated
    CompletableFuture<Void> setPersistenceAsync(String topic, PersistencePolicies persistencePolicies);

    /**
     * Get the configuration of persistence policies for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    PersistencePolicies getPersistence(String topic) throws PulsarAdminException;

    /**
     * Get the configuration of persistence policies for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    @Deprecated
    CompletableFuture<PersistencePolicies> getPersistenceAsync(String topic);

    /**
     * Get the applied configuration of persistence policies for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    PersistencePolicies getPersistence(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the applied configuration of persistence policies for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    @Deprecated
    CompletableFuture<PersistencePolicies> getPersistenceAsync(String topic, boolean applied);

    /**
     * Remove the configuration of persistence policies for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    void removePersistence(String topic) throws PulsarAdminException;

    /**
     * Remove the configuration of persistence policies for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    @Deprecated
    CompletableFuture<Void> removePersistenceAsync(String topic);

    /**
     * get deduplication enabled of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    Boolean getDeduplicationEnabled(String topic) throws PulsarAdminException;

    /**
     * get deduplication enabled of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Boolean> getDeduplicationEnabledAsync(String topic);

    /**
     * get deduplication enabled of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    Boolean getDeduplicationStatus(String topic) throws PulsarAdminException;

    /**
     * get deduplication enabled of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Boolean> getDeduplicationStatusAsync(String topic);
    /**
     * get applied deduplication enabled of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    Boolean getDeduplicationStatus(String topic, boolean applied) throws PulsarAdminException;

    /**
     * get applied deduplication enabled of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Boolean> getDeduplicationStatusAsync(String topic, boolean applied);

    /**
     * set deduplication enabled of a topic.
     * @param topic
     * @param enabled
     * @throws PulsarAdminException
     */
    @Deprecated
    void enableDeduplication(String topic, boolean enabled) throws PulsarAdminException;

    /**
     * set deduplication enabled of a topic asynchronously.
     * @param topic
     * @param enabled
     * @return
     */
    @Deprecated
    CompletableFuture<Void> enableDeduplicationAsync(String topic, boolean enabled);

    /**
     * set deduplication enabled of a topic.
     * @param topic
     * @param enabled
     * @throws PulsarAdminException
     */
    @Deprecated
    void setDeduplicationStatus(String topic, boolean enabled) throws PulsarAdminException;

    /**
     * set deduplication enabled of a topic asynchronously.
     * @param topic
     * @param enabled
     * @return
     */
    @Deprecated
    CompletableFuture<Void> setDeduplicationStatusAsync(String topic, boolean enabled);

    /**
     * remove deduplication enabled of a topic.
     * @param topic
     * @throws PulsarAdminException
     */
    @Deprecated
    void disableDeduplication(String topic) throws PulsarAdminException;

    /**
     * remove deduplication enabled of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Void> disableDeduplicationAsync(String topic);

    /**
     * remove deduplication enabled of a topic.
     * @param topic
     * @throws PulsarAdminException
     */
    @Deprecated
    void removeDeduplicationStatus(String topic) throws PulsarAdminException;

    /**
     * remove deduplication enabled of a topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Void> removeDeduplicationStatusAsync(String topic);

    /**
     * Set message-dispatch-rate (topic can dispatch this many messages per second).
     *
     * @param topic
     * @param dispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void setDispatchRate(String topic, DispatchRate dispatchRate) throws PulsarAdminException;

    /**
     * Set message-dispatch-rate asynchronously.
     * <p/>
     * topic can dispatch this many messages per second
     *
     * @param topic
     * @param dispatchRate
     *            number of messages per second
     */
    @Deprecated
    CompletableFuture<Void> setDispatchRateAsync(String topic, DispatchRate dispatchRate);

    /**
     * Get message-dispatch-rate (topic can dispatch this many messages per second).
     *
     * @param topic
     * @returns messageRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    DispatchRate getDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Get message-dispatch-rate asynchronously.
     * <p/>
     * Topic can dispatch this many messages per second.
     *
     * @param topic
     * @returns messageRate
     *            number of messages per second
     */
    @Deprecated
    CompletableFuture<DispatchRate> getDispatchRateAsync(String topic);

    /**
     * Get applied message-dispatch-rate (topic can dispatch this many messages per second).
     *
     * @param topic
     * @returns messageRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    DispatchRate getDispatchRate(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get applied message-dispatch-rate asynchronously.
     * <p/>
     * Topic can dispatch this many messages per second.
     *
     * @param topic
     * @returns messageRate
     *            number of messages per second
     */
    @Deprecated
    CompletableFuture<DispatchRate> getDispatchRateAsync(String topic, boolean applied);

    /**
     * Remove message-dispatch-rate.
     * <p/>
     * Remove topic message dispatch rate
     *
     * @param topic
     * @throws PulsarAdminException
     *              unexpected error
     */
    @Deprecated
    void removeDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Remove message-dispatch-rate asynchronously.
     * <p/>
     * Remove topic message dispatch rate
     *
     * @param topic
     * @throws PulsarAdminException
     *              unexpected error
     */
    @Deprecated
    CompletableFuture<Void> removeDispatchRateAsync(String topic) throws PulsarAdminException;

    /**
     * Set subscription-message-dispatch-rate for the topic.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second
     *
     * @param topic
     * @param dispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void setSubscriptionDispatchRate(String topic, DispatchRate dispatchRate) throws PulsarAdminException;

    /**
     * Set subscription-message-dispatch-rate for the topic asynchronously.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second.
     *
     * @param topic
     * @param dispatchRate
     *            number of messages per second
     */
    @Deprecated
    CompletableFuture<Void> setSubscriptionDispatchRateAsync(String topic, DispatchRate dispatchRate);

    /**
     * Get applied subscription-message-dispatch-rate.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second.
     *
     * @param namespace
     * @returns DispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    DispatchRate getSubscriptionDispatchRate(String namespace, boolean applied) throws PulsarAdminException;

    /**
     * Get applied subscription-message-dispatch-rate asynchronously.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second.
     *
     * @param namespace
     * @returns DispatchRate
     *            number of messages per second
     */
    @Deprecated
    CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String namespace, boolean applied);

    /**
     * Get subscription-message-dispatch-rate for the topic.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second.
     *
     * @param topic
     * @returns DispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    DispatchRate getSubscriptionDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Get subscription-message-dispatch-rate asynchronously.
     * <p/>
     * Subscriptions under this namespace can dispatch this many messages per second.
     *
     * @param topic
     * @returns DispatchRate
     *            number of messages per second
     */
    @Deprecated
    CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String topic);

    /**
     * Remove subscription-message-dispatch-rate for a topic.
     * @param topic
     *            Topic name
     * @throws PulsarAdminException
     *            Unexpected error
     */
    @Deprecated
    void removeSubscriptionDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Remove subscription-message-dispatch-rate for a topic asynchronously.
     * @param topic
     *            Topic name
     */
    @Deprecated
    CompletableFuture<Void> removeSubscriptionDispatchRateAsync(String topic);

    /**
     * Set replicatorDispatchRate for the topic.
     * <p/>
     * Replicator dispatch rate under this topic can dispatch this many messages per second
     *
     * @param topic
     * @param dispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void setReplicatorDispatchRate(String topic, DispatchRate dispatchRate) throws PulsarAdminException;

    /**
     * Set replicatorDispatchRate for the topic asynchronously.
     * <p/>
     * Replicator dispatch rate under this topic can dispatch this many messages per second.
     *
     * @param topic
     * @param dispatchRate
     *            number of messages per second
     */
    @Deprecated
    CompletableFuture<Void> setReplicatorDispatchRateAsync(String topic, DispatchRate dispatchRate);

    /**
     * Get replicatorDispatchRate for the topic.
     * <p/>
     * Replicator dispatch rate under this topic can dispatch this many messages per second.
     *
     * @param topic
     * @returns DispatchRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    DispatchRate getReplicatorDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Get replicatorDispatchRate asynchronously.
     * <p/>
     * Replicator dispatch rate under this topic can dispatch this many messages per second.
     *
     * @param topic
     * @returns DispatchRate
     *            number of messages per second
     */
    @Deprecated
    CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String topic);

    /**
     * Get applied replicatorDispatchRate for the topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    DispatchRate getReplicatorDispatchRate(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get applied replicatorDispatchRate asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    @Deprecated
    CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String topic, boolean applied);

    /**
     * Remove replicatorDispatchRate for a topic.
     * @param topic
     *            Topic name
     * @throws PulsarAdminException
     *            Unexpected error
     */
    @Deprecated
    void removeReplicatorDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Remove replicatorDispatchRate for a topic asynchronously.
     * @param topic
     *            Topic name
     */
    @Deprecated
    CompletableFuture<Void> removeReplicatorDispatchRateAsync(String topic);

    /**
     * Get the compactionThreshold for a topic. The maximum number of bytes
     * can have before compaction is triggered. 0 disables.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    Long getCompactionThreshold(String topic) throws PulsarAdminException;

    /**
     * Get the compactionThreshold for a topic asynchronously. The maximum number of bytes
     * can have before compaction is triggered. 0 disables.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     */
    @Deprecated
    CompletableFuture<Long> getCompactionThresholdAsync(String topic);

    /**
     * Get the compactionThreshold for a topic. The maximum number of bytes
     * can have before compaction is triggered. 0 disables.
     * @param topic Topic name
     * @throws NotAuthorizedException Don't have admin permission
     * @throws NotFoundException Namespace does not exist
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    Long getCompactionThreshold(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the compactionThreshold for a topic asynchronously. The maximum number of bytes
     * can have before compaction is triggered. 0 disables.
     * @param topic Topic name
     */
    @Deprecated
    CompletableFuture<Long> getCompactionThresholdAsync(String topic, boolean applied);

    /**
     * Set the compactionThreshold for a topic. The maximum number of bytes
     * can have before compaction is triggered. 0 disables.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     * @param compactionThreshold
     *            maximum number of backlog bytes before compaction is triggered
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void setCompactionThreshold(String topic, long compactionThreshold) throws PulsarAdminException;

    /**
     * Set the compactionThreshold for a topic asynchronously. The maximum number of bytes
     * can have before compaction is triggered. 0 disables.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>10000000</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     * @param compactionThreshold
     *            maximum number of backlog bytes before compaction is triggered
     */
    @Deprecated
    CompletableFuture<Void> setCompactionThresholdAsync(String topic, long compactionThreshold);

    /**
     * Remove the compactionThreshold for a topic.
     * @param topic
     *            Topic name
     * @throws PulsarAdminException
     *            Unexpected error
     */
    @Deprecated
    void removeCompactionThreshold(String topic) throws PulsarAdminException;

    /**
     * Remove the compactionThreshold for a topic asynchronously.
     * @param topic
     *            Topic name
     */
    @Deprecated
    CompletableFuture<Void> removeCompactionThresholdAsync(String topic);

    /**
     * Set message-publish-rate (topics can publish this many messages per second).
     *
     * @param topic
     * @param publishMsgRate
     *            number of messages per second
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void setPublishRate(String topic, PublishRate publishMsgRate) throws PulsarAdminException;

    /**
     * Set message-publish-rate (topics can publish this many messages per second) asynchronously.
     *
     * @param topic
     * @param publishMsgRate
     *            number of messages per second
     */
    @Deprecated
    CompletableFuture<Void> setPublishRateAsync(String topic, PublishRate publishMsgRate);

    /**
     * Get message-publish-rate (topics can publish this many messages per second).
     *
     * @param topic
     * @return number of messages per second
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    PublishRate getPublishRate(String topic) throws PulsarAdminException;

    /**
     * Get message-publish-rate (topics can publish this many messages per second) asynchronously.
     *
     * @param topic
     * @return number of messages per second
     */
    @Deprecated
    CompletableFuture<PublishRate> getPublishRateAsync(String topic);

    /**
     * Remove message-publish-rate.
     * <p/>
     * Remove topic message publish rate
     *
     * @param topic
     * @throws PulsarAdminException
     *              unexpected error
     */
    @Deprecated
    void removePublishRate(String topic) throws PulsarAdminException;

    /**
     * Remove message-publish-rate asynchronously.
     * <p/>
     * Remove topic message publish rate
     *
     * @param topic
     * @throws PulsarAdminException
     *              unexpected error
     */
    @Deprecated
    CompletableFuture<Void> removePublishRateAsync(String topic) throws PulsarAdminException;

    /**
     * Get the maxConsumersPerSubscription for a topic.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>0</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    Integer getMaxConsumersPerSubscription(String topic) throws PulsarAdminException;

    /**
     * Get the maxConsumersPerSubscription for a topic asynchronously.
     * <p/>
     * Response example:
     *
     * <pre>
     * <code>0</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     */
    @Deprecated
    CompletableFuture<Integer> getMaxConsumersPerSubscriptionAsync(String topic);

    /**
     * Set maxConsumersPerSubscription for a topic.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>10</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     * @param maxConsumersPerSubscription
     *            maxConsumersPerSubscription value for a namespace
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void setMaxConsumersPerSubscription(String topic, int maxConsumersPerSubscription) throws PulsarAdminException;

    /**
     * Set maxConsumersPerSubscription for a topic asynchronously.
     * <p/>
     * Request example:
     *
     * <pre>
     * <code>10</code>
     * </pre>
     *
     * @param topic
     *            Topic name
     * @param maxConsumersPerSubscription
     *            maxConsumersPerSubscription value for a namespace
     */
    @Deprecated
    CompletableFuture<Void> setMaxConsumersPerSubscriptionAsync(String topic, int maxConsumersPerSubscription);

    /**
     * Remove the maxConsumersPerSubscription for a topic.
     * @param topic
     *            Topic name
     * @throws PulsarAdminException
     *            Unexpected error
     */
    @Deprecated
    void removeMaxConsumersPerSubscription(String topic) throws PulsarAdminException;

    /**
     * Remove the maxConsumersPerSubscription for a topic asynchronously.
     * @param topic
     *            Topic name
     */
    @Deprecated
    CompletableFuture<Void> removeMaxConsumersPerSubscriptionAsync(String topic);

    /**
     * Get the max number of producer for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    Integer getMaxProducers(String topic) throws PulsarAdminException;

    /**
     * Get the max number of producer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    CompletableFuture<Integer> getMaxProducersAsync(String topic);

    /**
     * Get the max number of producer applied for specified topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    Integer getMaxProducers(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the max number of producer applied for specified topic asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    @Deprecated
    CompletableFuture<Integer> getMaxProducersAsync(String topic, boolean applied);


    /**
     * Set the max number of producer for specified topic.
     *
     * @param topic Topic name
     * @param maxProducers Max number of producer
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    void setMaxProducers(String topic, int maxProducers) throws PulsarAdminException;

    /**
     * Set the max number of producer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @param maxProducers Max number of producer
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    CompletableFuture<Void> setMaxProducersAsync(String topic, int maxProducers);

    /**
     * Remove the max number of producer for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    void removeMaxProducers(String topic) throws PulsarAdminException;

    /**
     * Remove the max number of producer for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    @Deprecated
    CompletableFuture<Void> removeMaxProducersAsync(String topic);

    /**
     * Get the max number of subscriptions for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    Integer getMaxSubscriptionsPerTopic(String topic) throws PulsarAdminException;

    /**
     * Get the max number of subscriptions for specified topic asynchronously.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    CompletableFuture<Integer> getMaxSubscriptionsPerTopicAsync(String topic);


    /**
     * Set the max number of subscriptions for specified topic.
     *
     * @param topic Topic name
     * @param maxSubscriptionsPerTopic Max number of subscriptions
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    void setMaxSubscriptionsPerTopic(String topic, int maxSubscriptionsPerTopic) throws PulsarAdminException;

    /**
     * Set the max number of subscriptions for specified topic asynchronously.
     *
     * @param topic Topic name
     * @param maxSubscriptionsPerTopic Max number of subscriptions
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    CompletableFuture<Void> setMaxSubscriptionsPerTopicAsync(String topic, int maxSubscriptionsPerTopic);

    /**
     * Remove the max number of subscriptions for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    void removeMaxSubscriptionsPerTopic(String topic) throws PulsarAdminException;

    /**
     * Remove the max number of subscriptions for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    @Deprecated
    CompletableFuture<Void> removeMaxSubscriptionsPerTopicAsync(String topic);

    /**
     * Get the max message size for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    Integer getMaxMessageSize(String topic) throws PulsarAdminException;

    /**
     * Get the max message size for specified topic asynchronously.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    CompletableFuture<Integer> getMaxMessageSizeAsync(String topic);


    /**
     * Set the max message size for specified topic.
     *
     * @param topic Topic name
     * @param maxMessageSize Max message size of producer
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    void setMaxMessageSize(String topic, int maxMessageSize) throws PulsarAdminException;

    /**
     * Set the max message size for specified topic asynchronously.0 disables.
     *
     * @param topic Topic name
     * @param maxMessageSize Max message size of topic
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    CompletableFuture<Void> setMaxMessageSizeAsync(String topic, int maxMessageSize);

    /**
     * Remove the max message size for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    void removeMaxMessageSize(String topic) throws PulsarAdminException;

    /**
     * Remove the max message size for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    @Deprecated
    CompletableFuture<Void> removeMaxMessageSizeAsync(String topic);

    /**
     * Get the max number of consumer for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    Integer getMaxConsumers(String topic) throws PulsarAdminException;

    /**
     * Get the max number of consumer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    CompletableFuture<Integer> getMaxConsumersAsync(String topic);

    /**
     * Get the max number of consumer applied for specified topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    Integer getMaxConsumers(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the max number of consumer applied for specified topic asynchronously.
     * @param topic
     * @param applied
     * @return
     */
    @Deprecated
    CompletableFuture<Integer> getMaxConsumersAsync(String topic, boolean applied);

    /**
     * Set the max number of consumer for specified topic.
     *
     * @param topic Topic name
     * @param maxConsumers Max number of consumer
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    void setMaxConsumers(String topic, int maxConsumers) throws PulsarAdminException;

    /**
     * Set the max number of consumer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @param maxConsumers Max number of consumer
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    CompletableFuture<Void> setMaxConsumersAsync(String topic, int maxConsumers);

    /**
     * Remove the max number of consumer for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    void removeMaxConsumers(String topic) throws PulsarAdminException;

    /**
     * Remove the max number of consumer for specified topic asynchronously.
     *
     * @param topic Topic name
     */
    @Deprecated
    CompletableFuture<Void> removeMaxConsumersAsync(String topic);

    /**
     * Get the deduplication snapshot interval for specified topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    @Deprecated
    Integer getDeduplicationSnapshotInterval(String topic) throws PulsarAdminException;

    /**
     * Get the deduplication snapshot interval for specified topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Integer> getDeduplicationSnapshotIntervalAsync(String topic);

    /**
     * Set the deduplication snapshot interval for specified topic.
     * @param topic
     * @param interval
     * @throws PulsarAdminException
     */
    @Deprecated
    void setDeduplicationSnapshotInterval(String topic, int interval) throws PulsarAdminException;

    /**
     * Set the deduplication snapshot interval for specified topic asynchronously.
     * @param topic
     * @param interval
     * @return
     */
    @Deprecated
    CompletableFuture<Void> setDeduplicationSnapshotIntervalAsync(String topic, int interval);

    /**
     * Remove the deduplication snapshot interval for specified topic.
     * @param topic
     * @throws PulsarAdminException
     */
    @Deprecated
    void removeDeduplicationSnapshotInterval(String topic) throws PulsarAdminException;

    /**
     * Remove the deduplication snapshot interval for specified topic asynchronously.
     * @param topic
     * @return
     */
    @Deprecated
    CompletableFuture<Void> removeDeduplicationSnapshotIntervalAsync(String topic);

    /**
     * Set is enable sub types.
     *
     * @param topic
     * @param subscriptionTypesEnabled
     *            is enable subTypes
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void setSubscriptionTypesEnabled(String topic,
                                     Set<SubscriptionType> subscriptionTypesEnabled) throws PulsarAdminException;

    /**
     * Set is enable sub types asynchronously.
     *
     * @param topic
     * @param subscriptionTypesEnabled
     *            is enable subTypes
     */
    @Deprecated
    CompletableFuture<Void> setSubscriptionTypesEnabledAsync(String topic,
                                                             Set<SubscriptionType> subscriptionTypesEnabled);

    /**
     * Get is enable sub types.
     *
     * @param topic
     *            is topic for get is enable sub types
     * @return set of enable sub types {@link Set<SubscriptionType>}
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    Set<SubscriptionType> getSubscriptionTypesEnabled(String topic) throws PulsarAdminException;

    /**
     * Get is enable sub types asynchronously.
     *
     * @param topic
     *            is topic for get is enable sub types
     */
    @Deprecated
    CompletableFuture<Set<SubscriptionType>> getSubscriptionTypesEnabledAsync(String topic);

    /**
     * Remove subscription types enabled for a topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     */
    @Deprecated
    void removeSubscriptionTypesEnabled(String topic) throws PulsarAdminException;

    /**
     * Remove subscription types enabled for a topic asynchronously.
     *
     * @param topic Topic name
     */
    @Deprecated
    CompletableFuture<Void> removeSubscriptionTypesEnabledAsync(String topic);

    /**
     * Set topic-subscribe-rate (topic will limit by subscribeRate).
     *
     * @param topic
     * @param subscribeRate
     *            consumer subscribe limit by this subscribeRate
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    void setSubscribeRate(String topic, SubscribeRate subscribeRate) throws PulsarAdminException;

    /**
     * Set topic-subscribe-rate (topics will limit by subscribeRate) asynchronously.
     *
     * @param topic
     * @param subscribeRate
     *            consumer subscribe limit by this subscribeRate
     */
    @Deprecated
    CompletableFuture<Void> setSubscribeRateAsync(String topic, SubscribeRate subscribeRate);

    /**
     * Get topic-subscribe-rate (topics allow subscribe times per consumer in a period).
     *
     * @param topic
     * @returns subscribeRate
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    SubscribeRate getSubscribeRate(String topic) throws PulsarAdminException;

    /**
     * Get topic-subscribe-rate asynchronously.
     * <p/>
     * Topic allow subscribe times per consumer in a period.
     *
     * @param topic
     * @returns subscribeRate
     */
    @Deprecated
    CompletableFuture<SubscribeRate> getSubscribeRateAsync(String topic);

    /**
     * Get applied topic-subscribe-rate (topics allow subscribe times per consumer in a period).
     *
     * @param topic
     * @returns subscribeRate
     * @throws PulsarAdminException
     *             Unexpected error
     */
    @Deprecated
    SubscribeRate getSubscribeRate(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get applied topic-subscribe-rate asynchronously.
     *
     * @param topic
     * @returns subscribeRate
     */
    @Deprecated
    CompletableFuture<SubscribeRate> getSubscribeRateAsync(String topic, boolean applied);

    /**
     * Remove topic-subscribe-rate.
     * <p/>
     * Remove topic subscribe rate
     *
     * @param topic
     * @throws PulsarAdminException
     *              unexpected error
     */
    @Deprecated
    void removeSubscribeRate(String topic) throws PulsarAdminException;

    /**
     * Remove topic-subscribe-rate asynchronously.
     * <p/>
     * Remove topic subscribe rate
     *
     * @param topic
     * @throws PulsarAdminException
     *              unexpected error
     */
    @Deprecated
    CompletableFuture<Void> removeSubscribeRateAsync(String topic) throws PulsarAdminException;

    /**
     * Examine a specific message on a topic by position relative to the earliest or the latest message.
     *
     * @param topic Topic name
     * @param initialPosition Relative start position to examine message. It can be 'latest' or 'earliest'
     * @param messagePosition The position of messages (default 1)
     */
    Message<byte[]> examineMessage(String topic, String initialPosition, long messagePosition)
            throws PulsarAdminException;

    /**
     * Examine a specific message on a topic by position relative to the earliest or the latest message.
     *
     * @param topic Topic name
     * @param initialPosition Relative start position to examine message. It can be 'latest' or 'earliest'
     * @param messagePosition The position of messages (default 1)
     */
    CompletableFuture<Message<byte[]>> examineMessageAsync(String topic, String initialPosition, long messagePosition)
            throws PulsarAdminException;

    /**
     * Truncate a topic.
     * <p/>
     *
     * @param topic
     *            topic name
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void truncate(String topic) throws PulsarAdminException;

    /**
     * Truncate a topic asynchronously.
     * <p/>
     * The latest ledger cannot be deleted.
     * <p/>
     *
     * @param topic
     *            topic name
     *
     * @return a future that can be used to track when the topic is truncated
     */
    CompletableFuture<Void> truncateAsync(String topic);

    /**
     * Enable or disable a replicated subscription on a topic.
     *
     * @param topic
     * @param subName
     * @param enabled
     * @throws PulsarAdminException
     */
    void setReplicatedSubscriptionStatus(String topic, String subName, boolean enabled) throws PulsarAdminException;

    /**
     * Enable or disable a replicated subscription on a topic asynchronously.
     *
     * @param topic
     * @param subName
     * @param enabled
     * @return
     */
    CompletableFuture<Void> setReplicatedSubscriptionStatusAsync(String topic, String subName, boolean enabled);

    /**
     * Get the replication clusters for a topic.
     *
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    Set<String> getReplicationClusters(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the replication clusters for a topic asynchronously.
     *
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     */
    CompletableFuture<Set<String>> getReplicationClustersAsync(String topic, boolean applied);

    /**
     * Set the replication clusters for the topic.
     *
     * @param topic
     * @param clusterIds
     * @return
     * @throws PulsarAdminException
     */
    void setReplicationClusters(String topic, List<String> clusterIds) throws PulsarAdminException;

    /**
     * Set the replication clusters for the topic asynchronously.
     *
     * @param topic
     * @param clusterIds
     * @return
     * @throws PulsarAdminException
     */
    CompletableFuture<Void> setReplicationClustersAsync(String topic, List<String> clusterIds);

    /**
     * Remove the replication clusters for the topic.
     *
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    void removeReplicationClusters(String topic) throws PulsarAdminException;

    /**
     * Remove the replication clusters for the topic asynchronously.
     *
     * @param topic
     * @return
     * @throws PulsarAdminException
     */
    CompletableFuture<Void> removeReplicationClustersAsync(String topic);

    /**
     * Get replicated subscription status on a topic.
     *
     * @param topic topic name
     * @param subName subscription name
     * @return a map of replicated subscription status on a topic
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    Map<String, Boolean> getReplicatedSubscriptionStatus(String topic, String subName) throws PulsarAdminException;

    /**
     * Get replicated subscription status on a topic asynchronously.
     *
     * @param topic topic name
     * @return a map of replicated subscription status on a topic
     */
    CompletableFuture<Map<String, Boolean>> getReplicatedSubscriptionStatusAsync(String topic, String subName);
}
