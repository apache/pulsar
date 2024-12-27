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
package org.apache.pulsar.client.admin;

import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.pulsar.client.api.TransactionIsolationLevel;
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
import org.apache.pulsar.common.stats.AnalyzeSubscriptionBacklogResult;

/**
 * Admin interface for Topics management.
 */
public interface Topics {

    enum QueryParam {
        Bundle("bundle");

        public final String value;

        private QueryParam(String value) {
            this.value = value;
        }
    }
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
     * @deprecated use {@link #getList(String, TopicDomain, ListTopicsOptions)} instead.
     */
    @Deprecated
    List<String> getList(String namespace, TopicDomain topicDomain, Map<QueryParam, Object> params)
            throws PulsarAdminException;

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
     * @param options
     *            params to query the topics
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
    List<String> getList(String namespace, TopicDomain topicDomain, ListTopicsOptions options)
            throws PulsarAdminException;

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
     * @deprecated use {@link #getListAsync(String, TopicDomain, ListTopicsOptions)} instead.
     */
    @Deprecated
    CompletableFuture<List<String>> getListAsync(String namespace, TopicDomain topicDomain,
                                                 Map<QueryParam, Object> params);
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
     * @param options
     *            params to get the topics
     *
     * @return a list of topics
     */
    CompletableFuture<List<String>> getListAsync(String namespace, TopicDomain topicDomain, ListTopicsOptions options);

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
     * @param options
     *            params to get the topics
     * @return a list of partitioned topics
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Namespace does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<String> getPartitionedTopicList(String namespace, ListTopicsOptions options) throws PulsarAdminException;

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
     * @param options
     *           params to filter the results
     * @return a list of partitioned topics
     */
    CompletableFuture<List<String>> getPartitionedTopicListAsync(String namespace, ListTopicsOptions options);

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
    default void createPartitionedTopic(String topic, int numPartitions) throws PulsarAdminException {
        createPartitionedTopic(topic, numPartitions, null);
    }

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
     * @param properties
     *            topic properties
     * @throws PulsarAdminException
     */
    void createPartitionedTopic(String topic, int numPartitions, Map<String, String> properties)
            throws PulsarAdminException;

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
    default CompletableFuture<Void> createPartitionedTopicAsync(String topic, int numPartitions) {
        return createPartitionedTopicAsync(topic, numPartitions, null);
    }

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
     * @param properties
     *            Topic properties
     * @return a future that can be used to track when the partitioned topic is created
     */
    CompletableFuture<Void> createPartitionedTopicAsync(String topic, int numPartitions,
                                                        Map<String, String> properties);

    /**
     * Create a non-partitioned topic.
     * <p/>
     * Create a non-partitioned topic.
     * <p/>
     *
     * @param topic Topic name
     * @throws PulsarAdminException
     */
    default void createNonPartitionedTopic(String topic) throws PulsarAdminException {
        createNonPartitionedTopic(topic, null);
    }

    /**
     * Create a non-partitioned topic.
     * <p/>
     * Create a non-partitioned topic.
     * <p/>
     *
     * @param topic Topic name
     * @param properties Topic properties
     * @throws PulsarAdminException
     */
    void createNonPartitionedTopic(String topic, Map<String, String> properties) throws PulsarAdminException;

    /**
     * Create a non-partitioned topic asynchronously.
     *
     * @param topic Topic name
     */
    default CompletableFuture<Void> createNonPartitionedTopicAsync(String topic) {
        return createNonPartitionedTopicAsync(topic, null);
    }

    /**
     * Create a non-partitioned topic asynchronously.
     *
     * @param topic Topic name
     * @param properties Topic properties
     */
    CompletableFuture<Void> createNonPartitionedTopicAsync(String topic, Map<String, String> properties);

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
     * Update number of partitions of a partitioned topic.
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
     * Update number of partitions of a partitioned topic asynchronously.
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
     * Update number of partitions of a partitioned topic.
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
     * Update number of partitions of a partitioned topic asynchronously.
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
     * Update number of partitions of a partitioned topic.
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
     * Update number of partitions of a partitioned topic asynchronously.
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
     * Get properties of a topic.
     * @param topic
     *            Topic name
     * @return Topic properties
     */
    Map<String, String> getProperties(String topic) throws PulsarAdminException;

    /**
     * Get properties of a topic asynchronously.
     * @param topic
     *            Topic name
     * @return a future that can be used to track when the topic properties is returned
     */
    CompletableFuture<Map<String, String>> getPropertiesAsync(String topic);

    /**
     * Update Topic Properties on a topic.
     * The new properties will override the existing values, old properties in the topic will be keep if not override.
     * @param topic
     * @param properties
     * @throws PulsarAdminException
     */
    void updateProperties(String topic, Map<String, String> properties) throws PulsarAdminException;

    /**
     * Update Topic Properties on a topic.
     * The new properties will override the existing values, old properties in the topic will be keep if not override.
     * @param topic
     * @param properties
     * @return
     */
    CompletableFuture<Void> updatePropertiesAsync(String topic, Map<String, String> properties);

    /**
     * Remove the key in properties on a topic.
     *
     * @param topic
     * @param key
     * @throws PulsarAdminException
     */
    void removeProperties(String topic, String key) throws PulsarAdminException;

    /**
     * Remove the key in properties on a topic asynchronously.
     *
     * @param topic
     * @param key
     * @return
     */
    CompletableFuture<Void> removePropertiesAsync(String topic, String key);

    /**
     * Delete a partitioned topic and its schemas.
     * <p/>
     * It will also delete all the partitions of the topic if it exists.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @param force
     *            Delete topic forcefully
     * @param deleteSchema
     *            Delete topic's schema storage and it is always true even if it is specified as false
     *
     * @throws PulsarAdminException
     * @deprecated Use {@link Topics#deletePartitionedTopic(String, boolean)} instead because parameter
     * `deleteSchema` is always true
     */
    @Deprecated
    void deletePartitionedTopic(String topic, boolean force, boolean deleteSchema) throws PulsarAdminException;

    /**
     * @see Topics#deletePartitionedTopic(String, boolean, boolean)
     * IMPORTANT NOTICE: the application is not able to connect to the topic(delete then re-create with same name) again
     * if the schema auto uploading is disabled. Besides, users should to use the truncate method to clean up
     * data of the topic instead of delete method if users continue to use this topic later.
     */
    default void deletePartitionedTopic(String topic, boolean force) throws PulsarAdminException {
        deletePartitionedTopic(topic, force, true);
    }

    /**
     * Delete a partitioned topic and its schemas asynchronously.
     * <p/>
     * It will also delete all the partitions of the topic if it exists.
     * <p/>
     *
     * @param topic
     *            Topic name
     * @param force
     *            Delete topic forcefully
     * @param deleteSchema
     *            Delete topic's schema storage and it is always true even if it is specified as false
     *
     * @return a future that can be used to track when the partitioned topic is deleted
     * @deprecated Use {@link Topics#deletePartitionedTopicAsync(String, boolean)} instead because parameter
     * `deleteSchema` is always true
     */
    @Deprecated
    CompletableFuture<Void> deletePartitionedTopicAsync(String topic, boolean force, boolean deleteSchema);

    /**
     * @see Topics#deletePartitionedTopic(String, boolean, boolean)
     */
    default CompletableFuture<Void> deletePartitionedTopicAsync(String topic, boolean force) {
        return deletePartitionedTopicAsync(topic, force, true);
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
     * Delete a topic and its schemas.
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
     *            Delete topic's schema storage and it is always true even if it is specified as false
     *
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic does not exist
     * @throws PreconditionFailedException
     *             Topic has active subscriptions or producers
     * @throws PulsarAdminException
     *             Unexpected error
     * @deprecated Use {@link Topics#delete(String, boolean)} instead because
     * parameter `deleteSchema` is always true
     */
    @Deprecated
    void delete(String topic, boolean force, boolean deleteSchema) throws PulsarAdminException;

    /**
     * @see Topics#delete(String, boolean, boolean)
     * IMPORTANT NOTICE: the application is not able to connect to the topic(delete then re-create with same name) again
     * if the schema auto uploading is disabled. Besides, users should to use the truncate method to clean up
     * data of the topic instead of delete method if users continue to use this topic later.
     */
    default void delete(String topic, boolean force) throws PulsarAdminException {
        delete(topic, force, true);
    }

    /**
     * Delete a topic and its schemas asynchronously.
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
     *            Delete topic's schema storage and it is always true even if it is specified as false
     *
     * @return a future that can be used to track when the topic is deleted
     * @deprecated Use {@link Topics#deleteAsync(String, boolean)} instead because
     * parameter `deleteSchema` is always true
     */
    @Deprecated
    CompletableFuture<Void> deleteAsync(String topic, boolean force, boolean deleteSchema);

    /**
     * @see Topics#deleteAsync(String, boolean, boolean)
     */
    default CompletableFuture<Void> deleteAsync(String topic, boolean force) {
        return deleteAsync(topic, force, true);
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
     * Terminate the partitioned topic and prevent any more messages being published on it.
     * <p/>
     *
     * @param topic
     *            topic name
     * @return the message id of the last message that was published in the each partition of topic
     */
    Map<Integer, MessageId> terminatePartitionedTopic(String topic) throws PulsarAdminException;

    /**
     * Terminate the partitioned topic and prevent any more messages being published on it.
     * <p/>
     *
     * @param topic
     *            topic name
     * @return the message id of the last message that was published in the each partition of topic
     */
    CompletableFuture<Map<Integer, MessageId>> terminatePartitionedTopicAsync(String topic);

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
                                              boolean subscriptionBacklogSize, boolean getEarliestTimeInBacklog)
            throws PulsarAdminException;

    default PartitionedTopicStats getPartitionedStats(String topic, boolean perPartition) throws PulsarAdminException {
        return getPartitionedStats(topic, perPartition, false, false, false);
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
     * @param getEarliestTimeInBacklog
     *            Whether to get the earliest time in backlog.
     * @return a future that can be used to track when the partitioned topic statistics are returned
     */
    CompletableFuture<PartitionedTopicStats> getPartitionedStatsAsync(
            String topic, boolean perPartition, boolean getPreciseBacklog, boolean subscriptionBacklogSize,
            boolean getEarliestTimeInBacklog);

    default CompletableFuture<PartitionedTopicStats> getPartitionedStatsAsync(String topic, boolean perPartition) {
        return getPartitionedStatsAsync(topic, perPartition, false, false, false);
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
    default List<Message<byte[]>> peekMessages(String topic, String subName, int numMessages)
            throws PulsarAdminException {
        return peekMessages(topic, subName, numMessages, false, TransactionIsolationLevel.READ_COMMITTED);
    }

    /**
     * Peek messages from a topic subscription.
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     * @param numMessages
     *            Number of messages
     * @param showServerMarker
     *            Enables the display of internal server write markers
     * @param transactionIsolationLevel
     *            Sets the isolation level for peeking messages within transactions.
     *            - 'READ_COMMITTED' allows peeking only committed transactional messages.
     *            - 'READ_UNCOMMITTED' allows peeking all messages,
     *                                 even transactional messages which have been aborted.
     * @return
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws NotFoundException
     *             Topic or subscription does not exist
     * @throws PulsarAdminException
     *             Unexpected error
     */
    List<Message<byte[]>> peekMessages(String topic, String subName, int numMessages,
                                       boolean showServerMarker, TransactionIsolationLevel transactionIsolationLevel)
            throws PulsarAdminException;

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
    default CompletableFuture<List<Message<byte[]>>> peekMessagesAsync(String topic, String subName, int numMessages) {
        return peekMessagesAsync(topic, subName, numMessages, false, TransactionIsolationLevel.READ_COMMITTED);
    }

    /**
     * Peek messages from a topic subscription asynchronously.
     *
     * @param topic
     *            topic name
     * @param subName
     *            Subscription name
     * @param numMessages
     *            Number of messages
     * @param showServerMarker
     *            Enables the display of internal server write markers
      @param transactionIsolationLevel
     *            Sets the isolation level for peeking messages within transactions.
     *            - 'READ_COMMITTED' allows peeking only committed transactional messages.
     *            - 'READ_UNCOMMITTED' allows peeking all messages,
     *                                 even transactional messages which have been aborted.
     * @return a future that can be used to track when the messages are returned
     */
    CompletableFuture<List<Message<byte[]>>> peekMessagesAsync(
            String topic, String subName, int numMessages,
            boolean showServerMarker, TransactionIsolationLevel transactionIsolationLevel);

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
     * @deprecated Using {@link #getMessagesById(String, long, long)} instead.
     */
    @Deprecated
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
     * @deprecated Using {@link #getMessagesByIdAsync(String, long, long)} instead.
     */
    @Deprecated
    CompletableFuture<Message<byte[]>> getMessageByIdAsync(String topic, long ledgerId, long entryId);

    /**
     * Get the messages by messageId.
     *
     * @param topic    Topic name
     * @param ledgerId Ledger id
     * @param entryId  Entry id
     * @return A set of messages.
     * @throws PulsarAdminException Unexpected error
     */
    List<Message<byte[]>> getMessagesById(String topic, long ledgerId, long entryId) throws PulsarAdminException;

    /**
     * Get the messages by messageId asynchronously.
     *
     * @param topic    Topic name
     * @param ledgerId Ledger id
     * @param entryId  Entry id
     * @return A future that can be used to track when a set of messages is returned.
     */
    CompletableFuture<List<Message<byte[]>>> getMessagesByIdAsync(String topic, long ledgerId, long entryId);

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
    default void createSubscription(String topic, String subscriptionName, MessageId messageId)
            throws PulsarAdminException {
        createSubscription(topic, subscriptionName, messageId, false);
    };

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
    default CompletableFuture<Void> createSubscriptionAsync(String topic, String subscriptionName,
                                                            MessageId messageId) {
        return createSubscriptionAsync(topic, subscriptionName, messageId, false);
    }

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
     * @param replicated
     *            replicated subscriptions.
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws ConflictException
     *             Subscription already exists
     * @throws NotAllowedException
     *             Command disallowed for requested resource
     * @throws PulsarAdminException
     *             Unexpected error
     */
    default void createSubscription(String topic, String subscriptionName, MessageId messageId, boolean replicated)
            throws PulsarAdminException {
        createSubscription(topic, subscriptionName, messageId, replicated, null);
    }

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
     * @param replicated
     *            replicated subscriptions.
     * @param properties
     *            subscription properties.
     * @throws NotAuthorizedException
     *             Don't have admin permission
     * @throws ConflictException
     *             Subscription already exists
     * @throws NotAllowedException
     *             Command disallowed for requested resource
     * @throws PulsarAdminException
     *             Unexpected error
     */
    void createSubscription(String topic, String subscriptionName, MessageId messageId, boolean replicated,
                            Map<String, String> properties)
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
     *
     * @param replicated
     *           replicated subscriptions.
     */
    default CompletableFuture<Void> createSubscriptionAsync(String topic, String subscriptionName, MessageId messageId,
                                                    boolean replicated) {
        return createSubscriptionAsync(topic, subscriptionName, messageId, replicated, null);
    }

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
     * @param replicated
     *           replicated subscriptions.
     * @param properties
     *            subscription properties.
     */
    CompletableFuture<Void> createSubscriptionAsync(String topic, String subscriptionName, MessageId messageId,
                                                    boolean replicated, Map<String, String> properties);

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
     * Update Subscription Properties on a topic subscription.
     * The new properties will override the existing values, properties that are not passed will be removed.
     * @param topic
     * @param subName
     * @param subscriptionProperties
     * @throws PulsarAdminException
     */
    void updateSubscriptionProperties(String topic, String subName, Map<String, String> subscriptionProperties)
            throws PulsarAdminException;

    /**
     * Get Subscription Properties on a topic subscription.
     * @param topic
     * @param subName
     * @throws PulsarAdminException
     */
    Map<String, String> getSubscriptionProperties(String topic, String subName)
            throws PulsarAdminException;

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
     * Update Subscription Properties on a topic subscription.
     * The new properties will override the existing values, properties that are not passed will be removed.
     * @param topic
     * @param subName
     * @param subscriptionProperties
     */
    CompletableFuture<Void>  updateSubscriptionPropertiesAsync(String topic, String subName,
                                                               Map<String, String> subscriptionProperties);

    /**
     * Get Subscription Properties on a topic subscription.
     * @param topic
     * @param subName
     */
    CompletableFuture<Map<String, String>> getSubscriptionPropertiesAsync(String topic, String subName);

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
     * Trigger topic trimming.
     * @param topic The topic to trim
     * @throws PulsarAdminException
     */
    void trimTopic(String topic) throws PulsarAdminException;

    /**
     * Trigger topic trimming asynchronously.
     * @param topic The topic to trim
     */
    CompletableFuture<Void> trimTopicAsync(String topic);

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
     * @deprecated Use {@link TopicPolicies#getBacklogQuotaMap(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#getBacklogQuotaMap(String, boolean)} instead.
     */
    @Deprecated
    Map<BacklogQuota.BacklogQuotaType, BacklogQuota> getBacklogQuotaMap(String topic, boolean applied)
            throws PulsarAdminException;

    /**
     * Analyze subscription backlog.
     * This is a potentially expensive operation, as it requires
     * to read the messages from storage.
     * This function takes into consideration batch messages
     * and also Subscription filters.
     * @param topic
     *            Topic name
     * @param subscriptionName
     *            the subscription
     * @param startPosition
     *           the position to start the scan from (empty means the last processed message)
     * @return an accurate analysis of the backlog
     * @throws PulsarAdminException
     *            Unexpected error
     */
    AnalyzeSubscriptionBacklogResult analyzeSubscriptionBacklog(String topic, String subscriptionName,
                                                                Optional<MessageId> startPosition)
            throws PulsarAdminException;

    /**
     * Analyze subscription backlog.
     * This is a potentially expensive operation, as it requires
     * to read the messages from storage.
     * This function takes into consideration batch messages
     * and also Subscription filters.
     * @param topic
     *            Topic name
     * @param subscriptionName
     *            the subscription
     * @param startPosition
     *           the position to start the scan from (empty means the last processed message)
     * @return an accurate analysis of the backlog
     * @throws PulsarAdminException
     *            Unexpected error
     */
    CompletableFuture<AnalyzeSubscriptionBacklogResult> analyzeSubscriptionBacklogAsync(String topic,
                                                                           String subscriptionName,
                                                                           Optional<MessageId> startPosition);

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
     * @deprecated Use {@link TopicPolicies#setBacklogQuota(String, BacklogQuota, BacklogQuota.BacklogQuotaType)}
     * instead.
     */
    @Deprecated
    void setBacklogQuota(String topic, BacklogQuota backlogQuota,
                         BacklogQuota.BacklogQuotaType backlogQuotaType) throws PulsarAdminException;

    /**
     * @deprecated Use {@link TopicPolicies#setBacklogQuota(String, BacklogQuota)} instead.
     */
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
     * @deprecated Use {@link TopicPolicies#removeBacklogQuota(String, BacklogQuota.BacklogQuotaType)} instead.
     */
    @Deprecated
    void removeBacklogQuota(String topic, BacklogQuota.BacklogQuotaType backlogQuotaType) throws PulsarAdminException;

    /**
     * @deprecated Use {@link TopicPolicies#removeBacklogQuota(String)} instead.
     */
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
     * @deprecated Use {@link TopicPolicies#getDelayedDeliveryPolicy(String, boolean)} instead.
     */
    @Deprecated
    DelayedDeliveryPolicies getDelayedDeliveryPolicy(String topic
            , boolean applied) throws PulsarAdminException;

    /**
     * Get the delayed delivery policy applied for a specified topic asynchronously.
     * @param topic
     * @param applied
     * @return
     * @deprecated Use {@link TopicPolicies#getDelayedDeliveryPolicyAsync(String, boolean)} instead.
     */
    @Deprecated
    CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryPolicyAsync(String topic
            , boolean applied);
    /**
     * Get the delayed delivery policy for a specified topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getDelayedDeliveryPolicy(String)} instead.
     */
    @Deprecated
    DelayedDeliveryPolicies getDelayedDeliveryPolicy(String topic) throws PulsarAdminException;

    /**
     * Get the delayed delivery policy for a specified topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#getDelayedDeliveryPolicyAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<DelayedDeliveryPolicies> getDelayedDeliveryPolicyAsync(String topic);

    /**
     * Set the delayed delivery policy for a specified topic.
     * @param topic
     * @param delayedDeliveryPolicies
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#setDelayedDeliveryPolicy(String, DelayedDeliveryPolicies)} instead.
     */
    @Deprecated
    void setDelayedDeliveryPolicy(String topic
            , DelayedDeliveryPolicies delayedDeliveryPolicies) throws PulsarAdminException;

    /**
     * Set the delayed delivery policy for a specified topic asynchronously.
     * @param topic
     * @param delayedDeliveryPolicies
     * @return
     * @deprecated Use {@link TopicPolicies#setDelayedDeliveryPolicyAsync(String, DelayedDeliveryPolicies)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setDelayedDeliveryPolicyAsync(String topic
            , DelayedDeliveryPolicies delayedDeliveryPolicies);

    /**
     * Remove the delayed delivery policy for a specified topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#removeDelayedDeliveryPolicyAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> removeDelayedDeliveryPolicyAsync(String topic);

    /**
     * Remove the delayed delivery policy for a specified topic.
     * @param topic
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#removeDelayedDeliveryPolicy(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#setMessageTTL(String, int)} instead.
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
     * @deprecated Use {@link TopicPolicies#getMessageTTL(String)} instead.
     */
    @Deprecated
    Integer getMessageTTL(String topic) throws PulsarAdminException;

    /**
     * Get message TTL applied for a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getMessageTTL(String, boolean)} instead.
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
     * @deprecated Use {@link TopicPolicies#removeMessageTTL(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#setRetention(String, RetentionPolicies)} instead.
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
     * @deprecated Use {@link TopicPolicies#setRetentionAsync(String, RetentionPolicies)} instead.
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
     * @deprecated Use {@link TopicPolicies#getRetention(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#getRetentionAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<RetentionPolicies> getRetentionAsync(String topic);

    /**
     * Get the applied retention configuration for a topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getRetention(String, boolean)} instead.
     */
    @Deprecated
    RetentionPolicies getRetention(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the applied retention configuration for a topic asynchronously.
     * @param topic
     * @param applied
     * @return
     * @deprecated Use {@link TopicPolicies#getRetentionAsync(String, boolean)} instead.
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
     * @deprecated Use {@link TopicPolicies#removeRetention(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#removeRetentionAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> removeRetentionAsync(String topic);

    /**
     * get max unacked messages on consumer of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getMaxUnackedMessagesOnConsumer(String)} instead.
     */
    @Deprecated
    Integer getMaxUnackedMessagesOnConsumer(String topic) throws PulsarAdminException;

    /**
     * get max unacked messages on consumer of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#getMaxUnackedMessagesOnConsumerAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Integer> getMaxUnackedMessagesOnConsumerAsync(String topic);

    /**
     * get applied max unacked messages on consumer of a topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getMaxUnackedMessagesOnConsumer(String, boolean)} instead.
     */
    @Deprecated
    Integer getMaxUnackedMessagesOnConsumer(String topic, boolean applied) throws PulsarAdminException;

    /**
     * get applied max unacked messages on consumer of a topic asynchronously.
     * @param topic
     * @param applied
     * @return
     * @deprecated Use {@link TopicPolicies#getMaxUnackedMessagesOnConsumerAsync(String, boolean)} instead.
     */
    @Deprecated
    CompletableFuture<Integer> getMaxUnackedMessagesOnConsumerAsync(String topic, boolean applied);

    /**
     * set max unacked messages on consumer of a topic.
     * @param topic
     * @param maxNum
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#setMaxUnackedMessagesOnConsumer(String, int)} instead.
     */
    @Deprecated
    void setMaxUnackedMessagesOnConsumer(String topic, int maxNum) throws PulsarAdminException;

    /**
     * set max unacked messages on consumer of a topic asynchronously.
     * @param topic
     * @param maxNum
     * @return
     * @deprecated Use {@link TopicPolicies#setMaxUnackedMessagesOnConsumerAsync(String, int)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setMaxUnackedMessagesOnConsumerAsync(String topic, int maxNum);

    /**
     * remove max unacked messages on consumer of a topic.
     * @param topic
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#removeMaxUnackedMessagesOnConsumer(String)} instead.
     */
    @Deprecated
    void removeMaxUnackedMessagesOnConsumer(String topic) throws PulsarAdminException;

    /**
     * remove max unacked messages on consumer of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#removeMaxUnackedMessagesOnConsumerAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> removeMaxUnackedMessagesOnConsumerAsync(String topic);

    /**
     * Get inactive topic policies applied for a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getInactiveTopicPolicies(String, boolean)} instead.
     */
    @Deprecated
    InactiveTopicPolicies getInactiveTopicPolicies(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get inactive topic policies applied for a topic asynchronously.
     * @param topic
     * @param applied
     * @return
     * @deprecated Use {@link TopicPolicies#getInactiveTopicPoliciesAsync(String, boolean)} instead.
     */
    @Deprecated
    CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String topic, boolean applied);
    /**
     * get inactive topic policies of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getInactiveTopicPolicies(String)} instead.
     */
    @Deprecated
    InactiveTopicPolicies getInactiveTopicPolicies(String topic) throws PulsarAdminException;

    /**
     * get inactive topic policies of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#getInactiveTopicPoliciesAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<InactiveTopicPolicies> getInactiveTopicPoliciesAsync(String topic);

    /**
     * set inactive topic policies of a topic.
     * @param topic
     * @param inactiveTopicPolicies
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#setInactiveTopicPolicies(String, InactiveTopicPolicies)} instead.
     */
    @Deprecated
    void setInactiveTopicPolicies(String topic
            , InactiveTopicPolicies inactiveTopicPolicies) throws PulsarAdminException;

    /**
     * set inactive topic policies of a topic asynchronously.
     * @param topic
     * @param inactiveTopicPolicies
     * @return
     * @deprecated Use {@link TopicPolicies#setInactiveTopicPoliciesAsync(String, InactiveTopicPolicies)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setInactiveTopicPoliciesAsync(String topic, InactiveTopicPolicies inactiveTopicPolicies);

    /**
     * remove inactive topic policies of a topic.
     * @param topic
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#removeInactiveTopicPolicies(String)} instead.
     */
    @Deprecated
    void removeInactiveTopicPolicies(String topic) throws PulsarAdminException;

    /**
     * remove inactive topic policies of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#removeInactiveTopicPoliciesAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> removeInactiveTopicPoliciesAsync(String topic);

    /**
     * get offload policies of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getOffloadPolicies(String)} instead.
     */
    @Deprecated
    OffloadPolicies getOffloadPolicies(String topic) throws PulsarAdminException;

    /**
     * get offload policies of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#getOffloadPoliciesAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String topic);

    /**
     * get applied offload policies of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getOffloadPolicies(String, boolean)} instead.
     */
    @Deprecated
    OffloadPolicies getOffloadPolicies(String topic, boolean applied) throws PulsarAdminException;

    /**
     * get applied offload policies of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#getOffloadPoliciesAsync(String, boolean)} instead.
     */
    @Deprecated
    CompletableFuture<OffloadPolicies> getOffloadPoliciesAsync(String topic, boolean applied);

    /**
     * set offload policies of a topic.
     * @param topic
     * @param offloadPolicies
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#setOffloadPolicies(String, OffloadPolicies)} instead.
     */
    @Deprecated
    void setOffloadPolicies(String topic, OffloadPolicies offloadPolicies) throws PulsarAdminException;

    /**
     * set offload policies of a topic asynchronously.
     * @param topic
     * @param offloadPolicies
     * @return
     * @deprecated Use {@link TopicPolicies#setOffloadPoliciesAsync(String, OffloadPolicies)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setOffloadPoliciesAsync(String topic, OffloadPolicies offloadPolicies);

    /**
     * remove offload policies of a topic.
     * @param topic
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#removeOffloadPolicies(String)} instead.
     */
    @Deprecated
    void removeOffloadPolicies(String topic) throws PulsarAdminException;

    /**
     * remove offload policies of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#removeOffloadPoliciesAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> removeOffloadPoliciesAsync(String topic);

    /**
     * get max unacked messages on subscription of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getMaxUnackedMessagesOnSubscription(String)} instead.
     */
    @Deprecated
    Integer getMaxUnackedMessagesOnSubscription(String topic) throws PulsarAdminException;

    /**
     * get max unacked messages on subscription of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#getMaxUnackedMessagesOnSubscriptionAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Integer> getMaxUnackedMessagesOnSubscriptionAsync(String topic);

    /**
     * get max unacked messages on subscription of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getMaxUnackedMessagesOnSubscriptionAsync(String, boolean)} instead.
     */
    @Deprecated
    Integer getMaxUnackedMessagesOnSubscription(String topic, boolean applied) throws PulsarAdminException;

    /**
     * get max unacked messages on subscription of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#getMaxUnackedMessagesOnSubscriptionAsync(String, boolean)} instead.
     */
    @Deprecated
    CompletableFuture<Integer> getMaxUnackedMessagesOnSubscriptionAsync(String topic, boolean applied);

    /**
     * set max unacked messages on subscription of a topic.
     * @param topic
     * @param maxNum
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#setMaxUnackedMessagesOnSubscription(String, int)} instead.
     */
    @Deprecated
    void setMaxUnackedMessagesOnSubscription(String topic, int maxNum) throws PulsarAdminException;

    /**
     * set max unacked messages on subscription of a topic asynchronously.
     * @param topic
     * @param maxNum
     * @return
     * @deprecated Use {@link TopicPolicies#setMaxUnackedMessagesOnSubscriptionAsync(String, int)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setMaxUnackedMessagesOnSubscriptionAsync(String topic, int maxNum);

    /**
     * remove max unacked messages on subscription of a topic.
     * @param topic
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#removeMaxUnackedMessagesOnSubscription(String)} instead.
     */
    @Deprecated
    void removeMaxUnackedMessagesOnSubscription(String topic) throws PulsarAdminException;

    /**
     * remove max unacked messages on subscription of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#removeMaxUnackedMessagesOnSubscriptionAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> removeMaxUnackedMessagesOnSubscriptionAsync(String topic);

    /**
     * Set the configuration of persistence policies for specified topic.
     *
     * @param topic Topic name
     * @param persistencePolicies Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#setPersistence(String, PersistencePolicies)} (String)} instead.
     */
    @Deprecated
    void setPersistence(String topic, PersistencePolicies persistencePolicies) throws PulsarAdminException;

    /**
     * Set the configuration of persistence policies for specified topic asynchronously.
     *
     * @param topic Topic name
     * @param persistencePolicies Configuration of bookkeeper persistence policies
     * @deprecated Use {@link TopicPolicies#setPersistenceAsync(String, PersistencePolicies)} (String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setPersistenceAsync(String topic, PersistencePolicies persistencePolicies);

    /**
     * Get the configuration of persistence policies for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#getPersistence(String)} instead.
     */
    @Deprecated
    PersistencePolicies getPersistence(String topic) throws PulsarAdminException;

    /**
     * Get the configuration of persistence policies for specified topic asynchronously.
     *
     * @param topic Topic name
     * @deprecated Use {@link TopicPolicies#getPersistenceAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<PersistencePolicies> getPersistenceAsync(String topic);

    /**
     * Get the applied configuration of persistence policies for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#getPersistence(String, boolean)} instead.
     */
    @Deprecated
    PersistencePolicies getPersistence(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the applied configuration of persistence policies for specified topic asynchronously.
     *
     * @param topic Topic name
     * @deprecated Use {@link TopicPolicies#getPersistenceAsync(String, boolean)} instead.
     */
    @Deprecated
    CompletableFuture<PersistencePolicies> getPersistenceAsync(String topic, boolean applied);

    /**
     * Remove the configuration of persistence policies for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#removePersistence(String)} instead.
     */
    @Deprecated
    void removePersistence(String topic) throws PulsarAdminException;

    /**
     * Remove the configuration of persistence policies for specified topic asynchronously.
     *
     * @param topic Topic name
     * @deprecated Use {@link TopicPolicies#removePersistenceAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> removePersistenceAsync(String topic);

    /**
     * get deduplication enabled of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getDeduplicationStatus(String)} instead.
     */
    @Deprecated
    Boolean getDeduplicationEnabled(String topic) throws PulsarAdminException;

    /**
     * get deduplication enabled of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#getDeduplicationStatusAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Boolean> getDeduplicationEnabledAsync(String topic);

    /**
     * get deduplication enabled of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getDeduplicationStatus(String)} instead.
     */
    @Deprecated
    Boolean getDeduplicationStatus(String topic) throws PulsarAdminException;

    /**
     * get deduplication enabled of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#getDeduplicationStatusAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Boolean> getDeduplicationStatusAsync(String topic);
    /**
     * get applied deduplication enabled of a topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getDeduplicationStatus(String, boolean)} instead.
     */
    @Deprecated
    Boolean getDeduplicationStatus(String topic, boolean applied) throws PulsarAdminException;

    /**
     * get applied deduplication enabled of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#getDeduplicationStatusAsync(String, boolean)} instead.
     */
    @Deprecated
    CompletableFuture<Boolean> getDeduplicationStatusAsync(String topic, boolean applied);

    /**
     * set deduplication enabled of a topic.
     * @param topic
     * @param enabled
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#setDeduplicationStatus(String, boolean)} instead.
     */
    @Deprecated
    void enableDeduplication(String topic, boolean enabled) throws PulsarAdminException;

    /**
     * set deduplication enabled of a topic asynchronously.
     * @param topic
     * @param enabled
     * @return
     * @deprecated Use {@link TopicPolicies#setDeduplicationStatusAsync(String, boolean)} instead.
     */
    @Deprecated
    CompletableFuture<Void> enableDeduplicationAsync(String topic, boolean enabled);

    /**
     * set deduplication enabled of a topic.
     * @param topic
     * @param enabled
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#setDeduplicationStatus(String, boolean)} instead.
     */
    @Deprecated
    void setDeduplicationStatus(String topic, boolean enabled) throws PulsarAdminException;

    /**
     * set deduplication enabled of a topic asynchronously.
     * @param topic
     * @param enabled
     * @return
     * @deprecated Use {@link TopicPolicies#setDeduplicationStatusAsync(String, boolean)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setDeduplicationStatusAsync(String topic, boolean enabled);

    /**
     * remove deduplication enabled of a topic.
     * @param topic
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#removeDeduplicationStatus(String)} instead.
     */
    @Deprecated
    void disableDeduplication(String topic) throws PulsarAdminException;

    /**
     * remove deduplication enabled of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#removeDeduplicationStatusAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> disableDeduplicationAsync(String topic);

    /**
     * remove deduplication enabled of a topic.
     * @param topic
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#removeDeduplicationStatus(String)} instead.
     */
    @Deprecated
    void removeDeduplicationStatus(String topic) throws PulsarAdminException;

    /**
     * remove deduplication enabled of a topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#removeDeduplicationStatusAsync(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#setDispatchRate(String, DispatchRate)} instead.
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
     * @deprecated Use {@link TopicPolicies#setDispatchRateAsync(String, DispatchRate)} instead.
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
     * @deprecated Use {@link TopicPolicies#getDispatchRate(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#getDispatchRateAsync(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#getDispatchRate(String, boolean)} instead.
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
     * @deprecated Use {@link TopicPolicies#getDispatchRateAsync(String, boolean)} instead.
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
     * @deprecated Use {@link TopicPolicies#removeDispatchRate(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#removeDispatchRateAsync(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#setSubscriptionDispatchRate(String, DispatchRate)} instead.
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
     * @deprecated Use {@link TopicPolicies#setSubscriptionDispatchRateAsync(String, DispatchRate)} instead.
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
     * @deprecated Use {@link TopicPolicies#getSubscriptionDispatchRate(String, boolean)} instead.
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
     * @deprecated Use {@link TopicPolicies#getSubscriptionDispatchRateAsync(String, boolean)} instead.
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
     * @deprecated Use {@link TopicPolicies#getSubscriptionDispatchRate(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#getSubscriptionDispatchRateAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<DispatchRate> getSubscriptionDispatchRateAsync(String topic);

    /**
     * Remove subscription-message-dispatch-rate for a topic.
     * @param topic
     *            Topic name
     * @throws PulsarAdminException
     *            Unexpected error
     * @deprecated Use {@link TopicPolicies#removeSubscriptionDispatchRate(String)} instead.
     */
    @Deprecated
    void removeSubscriptionDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Remove subscription-message-dispatch-rate for a topic asynchronously.
     * @param topic
     *            Topic name
     * @deprecated Use {@link TopicPolicies#removeSubscriptionDispatchRateAsync(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#setReplicatorDispatchRate(String, DispatchRate)} instead.
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
     * @deprecated Use {@link TopicPolicies#setReplicatorDispatchRateAsync(String, DispatchRate)} instead.
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
     * @deprecated Use {@link TopicPolicies#getReplicatorDispatchRate(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#getReplicatorDispatchRateAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String topic);

    /**
     * Get applied replicatorDispatchRate for the topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getReplicatorDispatchRate(String, boolean)} instead.
     */
    @Deprecated
    DispatchRate getReplicatorDispatchRate(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get applied replicatorDispatchRate asynchronously.
     * @param topic
     * @param applied
     * @return
     * @deprecated Use {@link TopicPolicies#getReplicatorDispatchRateAsync(String, boolean)} instead.
     */
    @Deprecated
    CompletableFuture<DispatchRate> getReplicatorDispatchRateAsync(String topic, boolean applied);

    /**
     * Remove replicatorDispatchRate for a topic.
     * @param topic
     *            Topic name
     * @throws PulsarAdminException
     *            Unexpected error
     * @deprecated Use {@link TopicPolicies#removeReplicatorDispatchRate(String)} instead.
     */
    @Deprecated
    void removeReplicatorDispatchRate(String topic) throws PulsarAdminException;

    /**
     * Remove replicatorDispatchRate for a topic asynchronously.
     * @param topic
     *            Topic name
     * @deprecated Use {@link TopicPolicies#removeReplicatorDispatchRateAsync(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#getCompactionThreshold(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#getCompactionThresholdAsync(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#getCompactionThreshold(String, boolean)} instead.
     */
    @Deprecated
    Long getCompactionThreshold(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the compactionThreshold for a topic asynchronously. The maximum number of bytes
     * can have before compaction is triggered. 0 disables.
     * @param topic Topic name
     * @deprecated Use {@link TopicPolicies#getCompactionThreshold(String, boolean)} instead.
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
     * @deprecated Use {@link TopicPolicies#setCompactionThreshold(String, long)} instead.
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
     * @deprecated Use {@link TopicPolicies#setCompactionThresholdAsync(String, long)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setCompactionThresholdAsync(String topic, long compactionThreshold);

    /**
     * Remove the compactionThreshold for a topic.
     * @param topic
     *            Topic name
     * @throws PulsarAdminException
     *            Unexpected error
     * @deprecated Use {@link TopicPolicies#removeCompactionThreshold(String)} instead.
     */
    @Deprecated
    void removeCompactionThreshold(String topic) throws PulsarAdminException;

    /**
     * Remove the compactionThreshold for a topic asynchronously.
     * @param topic
     *            Topic name
     * @deprecated Use {@link TopicPolicies#removeCompactionThresholdAsync(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#setPublishRate(String, PublishRate)} instead.
     */
    @Deprecated
    void setPublishRate(String topic, PublishRate publishMsgRate) throws PulsarAdminException;

    /**
     * Set message-publish-rate (topics can publish this many messages per second) asynchronously.
     *
     * @param topic
     * @param publishMsgRate
     *            number of messages per second
     * @deprecated Use {@link TopicPolicies#setPublishRateAsync(String, PublishRate)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setPublishRateAsync(String topic, PublishRate publishMsgRate);

    /**
     * Get message-publish-rate (topics can publish this many messages per second).
     *
     * @param topic
     * @return number of messages per second
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#getPublishRate(String)} instead.
     */
    @Deprecated
    PublishRate getPublishRate(String topic) throws PulsarAdminException;

    /**
     * Get message-publish-rate (topics can publish this many messages per second) asynchronously.
     *
     * @param topic
     * @return number of messages per second
     * @deprecated Use {@link TopicPolicies#getPublishRateAsync(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#removePublishRate(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#removePublishRateAsync(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#getMaxConsumersPerSubscription(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#getMaxConsumersPerSubscriptionAsync(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#setMaxConsumersPerSubscription(String, int)} instead.
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
     * @deprecated Use {@link TopicPolicies#setMaxConsumersPerSubscriptionAsync(String, int)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setMaxConsumersPerSubscriptionAsync(String topic, int maxConsumersPerSubscription);

    /**
     * Remove the maxConsumersPerSubscription for a topic.
     * @param topic
     *            Topic name
     * @throws PulsarAdminException
     *            Unexpected error
     * @deprecated Use {@link TopicPolicies#removeMaxConsumersPerSubscription(String)} instead.
     */
    @Deprecated
    void removeMaxConsumersPerSubscription(String topic) throws PulsarAdminException;

    /**
     * Remove the maxConsumersPerSubscription for a topic asynchronously.
     * @param topic
     *            Topic name
     * @deprecated Use {@link TopicPolicies#removeMaxConsumersPerSubscriptionAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> removeMaxConsumersPerSubscriptionAsync(String topic);

    /**
     * Get the max number of producer for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#getMaxProducers(String)} instead.
     */
    @Deprecated
    Integer getMaxProducers(String topic) throws PulsarAdminException;

    /**
     * Get the max number of producer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#getMaxProducersAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Integer> getMaxProducersAsync(String topic);

    /**
     * Get the max number of producer applied for specified topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getMaxProducers(String, boolean)} instead.
     */
    @Deprecated
    Integer getMaxProducers(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the max number of producer applied for specified topic asynchronously.
     * @param topic
     * @param applied
     * @return
     * @deprecated Use {@link TopicPolicies#getMaxProducersAsync(String, boolean)} instead.
     */
    @Deprecated
    CompletableFuture<Integer> getMaxProducersAsync(String topic, boolean applied);


    /**
     * Set the max number of producer for specified topic.
     *
     * @param topic Topic name
     * @param maxProducers Max number of producer
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#setMaxProducers(String, int)} instead.
     */
    @Deprecated
    void setMaxProducers(String topic, int maxProducers) throws PulsarAdminException;

    /**
     * Set the max number of producer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @param maxProducers Max number of producer
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#setMaxProducersAsync(String, int)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setMaxProducersAsync(String topic, int maxProducers);

    /**
     * Remove the max number of producer for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#removeMaxProducers(String)} instead.
     */
    @Deprecated
    void removeMaxProducers(String topic) throws PulsarAdminException;

    /**
     * Remove the max number of producer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @deprecated Use {@link TopicPolicies#removeMaxProducersAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> removeMaxProducersAsync(String topic);

    /**
     * Get the max number of subscriptions for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#getMaxSubscriptionsPerTopic(String)} instead.
     */
    @Deprecated
    Integer getMaxSubscriptionsPerTopic(String topic) throws PulsarAdminException;

    /**
     * Get the max number of subscriptions for specified topic asynchronously.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#getMaxSubscriptionsPerTopicAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Integer> getMaxSubscriptionsPerTopicAsync(String topic);


    /**
     * Set the max number of subscriptions for specified topic.
     *
     * @param topic Topic name
     * @param maxSubscriptionsPerTopic Max number of subscriptions
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#setMaxSubscriptionsPerTopic(String, int)} instead.
     */
    @Deprecated
    void setMaxSubscriptionsPerTopic(String topic, int maxSubscriptionsPerTopic) throws PulsarAdminException;

    /**
     * Set the max number of subscriptions for specified topic asynchronously.
     *
     * @param topic Topic name
     * @param maxSubscriptionsPerTopic Max number of subscriptions
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#setMaxSubscriptionsPerTopicAsync(String, int)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setMaxSubscriptionsPerTopicAsync(String topic, int maxSubscriptionsPerTopic);

    /**
     * Remove the max number of subscriptions for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#removeMaxSubscriptionsPerTopic(String)} instead.
     */
    @Deprecated
    void removeMaxSubscriptionsPerTopic(String topic) throws PulsarAdminException;

    /**
     * Remove the max number of subscriptions for specified topic asynchronously.
     *
     * @param topic Topic name
     * @deprecated Use {@link TopicPolicies#removeMaxSubscriptionsPerTopicAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> removeMaxSubscriptionsPerTopicAsync(String topic);

    /**
     * Get the max message size for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#getMaxMessageSize(String)} instead.
     */
    @Deprecated
    Integer getMaxMessageSize(String topic) throws PulsarAdminException;

    /**
     * Get the max message size for specified topic asynchronously.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#getMaxMessageSizeAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Integer> getMaxMessageSizeAsync(String topic);


    /**
     * Set the max message size for specified topic.
     *
     * @param topic Topic name
     * @param maxMessageSize Max message size of producer
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#setMaxMessageSize(String, int)} instead.
     */
    @Deprecated
    void setMaxMessageSize(String topic, int maxMessageSize) throws PulsarAdminException;

    /**
     * Set the max message size for specified topic asynchronously.0 disables.
     *
     * @param topic Topic name
     * @param maxMessageSize Max message size of topic
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#setMaxMessageSizeAsync(String, int)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setMaxMessageSizeAsync(String topic, int maxMessageSize);

    /**
     * Remove the max message size for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#removeMaxMessageSize(String)} instead.
     */
    @Deprecated
    void removeMaxMessageSize(String topic) throws PulsarAdminException;

    /**
     * Remove the max message size for specified topic asynchronously.
     *
     * @param topic Topic name
     * @deprecated Use {@link TopicPolicies#removeMaxMessageSizeAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> removeMaxMessageSizeAsync(String topic);

    /**
     * Get the max number of consumer for specified topic.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#getMaxConsumers(String)} instead.
     */
    @Deprecated
    Integer getMaxConsumers(String topic) throws PulsarAdminException;

    /**
     * Get the max number of consumer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @return Configuration of bookkeeper persistence policies
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#getMaxConsumersAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Integer> getMaxConsumersAsync(String topic);

    /**
     * Get the max number of consumer applied for specified topic.
     * @param topic
     * @param applied
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getMaxConsumers(String, boolean)} instead.
     */
    @Deprecated
    Integer getMaxConsumers(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get the max number of consumer applied for specified topic asynchronously.
     * @param topic
     * @param applied
     * @return
     * @deprecated Use {@link TopicPolicies#getMaxConsumersAsync(String, boolean)} instead.
     */
    @Deprecated
    CompletableFuture<Integer> getMaxConsumersAsync(String topic, boolean applied);

    /**
     * Set the max number of consumer for specified topic.
     *
     * @param topic Topic name
     * @param maxConsumers Max number of consumer
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#setMaxConsumers(String, int)} instead.
     */
    @Deprecated
    void setMaxConsumers(String topic, int maxConsumers) throws PulsarAdminException;

    /**
     * Set the max number of consumer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @param maxConsumers Max number of consumer
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#setMaxConsumers(String, int)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setMaxConsumersAsync(String topic, int maxConsumers);

    /**
     * Remove the max number of consumer for specified topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#removeMaxConsumers(String)} instead.
     */
    @Deprecated
    void removeMaxConsumers(String topic) throws PulsarAdminException;

    /**
     * Remove the max number of consumer for specified topic asynchronously.
     *
     * @param topic Topic name
     * @deprecated Use {@link TopicPolicies#removeMaxConsumersAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Void> removeMaxConsumersAsync(String topic);

    /**
     * Get the deduplication snapshot interval for specified topic.
     * @param topic
     * @return
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#getDeduplicationSnapshotInterval(String)} instead.
     */
    @Deprecated
    Integer getDeduplicationSnapshotInterval(String topic) throws PulsarAdminException;

    /**
     * Get the deduplication snapshot interval for specified topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#getDeduplicationSnapshotIntervalAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Integer> getDeduplicationSnapshotIntervalAsync(String topic);

    /**
     * Set the deduplication snapshot interval for specified topic.
     * @param topic
     * @param interval
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#setDeduplicationSnapshotInterval(String, int)} instead.
     */
    @Deprecated
    void setDeduplicationSnapshotInterval(String topic, int interval) throws PulsarAdminException;

    /**
     * Set the deduplication snapshot interval for specified topic asynchronously.
     * @param topic
     * @param interval
     * @return
     * @deprecated Use {@link TopicPolicies#setDeduplicationSnapshotIntervalAsync(String, int)} instead.
     */
    @Deprecated
    CompletableFuture<Void> setDeduplicationSnapshotIntervalAsync(String topic, int interval);

    /**
     * Remove the deduplication snapshot interval for specified topic.
     * @param topic
     * @throws PulsarAdminException
     * @deprecated Use {@link TopicPolicies#removeDeduplicationSnapshotInterval(String)} instead.
     */
    @Deprecated
    void removeDeduplicationSnapshotInterval(String topic) throws PulsarAdminException;

    /**
     * Remove the deduplication snapshot interval for specified topic asynchronously.
     * @param topic
     * @return
     * @deprecated Use {@link TopicPolicies#removeDeduplicationSnapshotIntervalAsync(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#setSubscriptionTypesEnabled(String, Set)} instead.
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
     * @deprecated Use {@link TopicPolicies#setSubscriptionTypesEnabledAsync(String, Set)} instead.
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
     * @deprecated Use {@link TopicPolicies#getSubscriptionTypesEnabled(String)} instead.
     */
    @Deprecated
    Set<SubscriptionType> getSubscriptionTypesEnabled(String topic) throws PulsarAdminException;

    /**
     * Get is enable sub types asynchronously.
     *
     * @param topic
     *            is topic for get is enable sub types
     * @deprecated Use {@link TopicPolicies#getSubscriptionTypesEnabledAsync(String)} instead.
     */
    @Deprecated
    CompletableFuture<Set<SubscriptionType>> getSubscriptionTypesEnabledAsync(String topic);

    /**
     * Remove subscription types enabled for a topic.
     *
     * @param topic Topic name
     * @throws PulsarAdminException Unexpected error
     * @deprecated Use {@link TopicPolicies#removeSubscriptionTypesEnabled(String)} instead.
     */
    @Deprecated
    void removeSubscriptionTypesEnabled(String topic) throws PulsarAdminException;

    /**
     * Remove subscription types enabled for a topic asynchronously.
     *
     * @param topic Topic name
     * @deprecated Use {@link TopicPolicies#removeSubscriptionTypesEnabledAsync(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#setSubscribeRate(String, SubscribeRate)} instead.
     */
    @Deprecated
    void setSubscribeRate(String topic, SubscribeRate subscribeRate) throws PulsarAdminException;

    /**
     * Set topic-subscribe-rate (topics will limit by subscribeRate) asynchronously.
     *
     * @param topic
     * @param subscribeRate
     *            consumer subscribe limit by this subscribeRate
     * @deprecated Use {@link TopicPolicies#setSubscribeRateAsync(String, SubscribeRate)} instead.
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
     * @deprecated Use {@link TopicPolicies#getSubscribeRate(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#getSubscribeRateAsync(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#getSubscribeRate(String, boolean)} instead.
     */
    @Deprecated
    SubscribeRate getSubscribeRate(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get applied topic-subscribe-rate asynchronously.
     *
     * @param topic
     * @returns subscribeRate
     * @deprecated Use {@link TopicPolicies#getSubscribeRateAsync(String, boolean)} instead.
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
     * @deprecated Use {@link TopicPolicies#removeSubscribeRate(String)} instead.
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
     * @deprecated Use {@link TopicPolicies#removeSubscribeRateAsync(String)} instead.
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
    void setReplicatedSubscriptionStatus(String topic, String subName, Boolean enabled) throws PulsarAdminException;

    /**
     * Enable or disable a replicated subscription on a topic asynchronously.
     *
     * @param topic
     * @param subName
     * @param enabled
     * @return
     */
    CompletableFuture<Void> setReplicatedSubscriptionStatusAsync(String topic, String subName, Boolean enabled);

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

    /**
     * Get schema validation enforced for a topic.
     *
     * @param topic topic name
     * @return whether the schema validation enforced is set or not
     */
    boolean getSchemaValidationEnforced(String topic, boolean applied) throws PulsarAdminException;

    /**
     * Get schema validation enforced for a topic.
     *
     * @param topic topic name
     */
    void setSchemaValidationEnforced(String topic, boolean enable) throws PulsarAdminException;

    /**
     * Get schema validation enforced for a topic asynchronously.
     *
     * @param topic topic name
     * @return whether the schema validation enforced is set or not
     */
    CompletableFuture<Boolean> getSchemaValidationEnforcedAsync(String topic, boolean applied);

    /**
     * Get schema validation enforced for a topic asynchronously.
     *
     * @param topic topic name
     */
    CompletableFuture<Void> setSchemaValidationEnforcedAsync(String topic, boolean enable);

    /**
     * Set shadow topic list for a source topic.
     *
     * @param sourceTopic  source topic name
     * @param shadowTopics list of shadow topic name
     */
    void setShadowTopics(String sourceTopic, List<String> shadowTopics) throws PulsarAdminException;

    /**
     * Remove all shadow topics for a source topic.
     *
     * @param sourceTopic source topic name
     */
    void removeShadowTopics(String sourceTopic) throws PulsarAdminException;

    /**
     * Get shadow topic list of the source topic.
     *
     * @param sourceTopic source topic name
     * @return shadow topic list
     */
    List<String> getShadowTopics(String sourceTopic) throws PulsarAdminException;

    /**
     * Set shadow topic list for a source topic asynchronously.
     *
     * @param sourceTopic source topic name
     */
    CompletableFuture<Void> setShadowTopicsAsync(String sourceTopic, List<String> shadowTopics);

    /**
     * Remove all shadow topics for a source topic asynchronously.
     *
     * @param sourceTopic source topic name
     */
    CompletableFuture<Void> removeShadowTopicsAsync(String sourceTopic);

    /**
     * Get shadow topic list of the source topic asynchronously.
     *
     * @param sourceTopic source topic name
     */
    CompletableFuture<List<String>> getShadowTopicsAsync(String sourceTopic);

    /**
     * Get the shadow source topic name of the given shadow topic.
     * @param shadowTopic shadow topic name.
     * @return The topic name of the source of the shadow topic.
     */
    String getShadowSource(String shadowTopic) throws PulsarAdminException;

    /**
     * Get the shadow source topic name of the given shadow topic asynchronously.
     * @param shadowTopic shadow topic name.
     * @return The topic name of the source of the shadow topic.
     */
    CompletableFuture<String> getShadowSourceAsync(String shadowTopic);

    /**
     * Create a new shadow topic as the shadow of the source topic.
     * The source topic must exist before call this method.
     * <p>
     *     For partitioned source topic, the partition number of shadow topic follows the source topic at creation. If
     *     the partition number of the source topic changes, the shadow topic needs to update its partition number
     *     manually.
     *     For non-partitioned source topic, the shadow topic will be created as non-partitioned topic.
     * </p>
     *
     * NOTE: This is still WIP until <a href="https://github.com/apache/pulsar/issues/16153">PIP-180</a> is finished.
     *
     * @param shadowTopic shadow topic name, and it must be a persistent topic name.
     * @param sourceTopic source topic name, and it must be a persistent topic name.
     * @param properties properties to be created with in the shadow topic.
     * @throws PulsarAdminException
     */
    void createShadowTopic(String shadowTopic, String sourceTopic, Map<String, String> properties)
            throws PulsarAdminException;

    /**
     * Create a new shadow topic, see #{@link #createShadowTopic(String, String, Map)} for details.
     */
    CompletableFuture<Void> createShadowTopicAsync(String shadowTopic, String sourceTopic,
                                                   Map<String, String> properties);

    /**
     * Create a new shadow topic, see #{@link #createShadowTopic(String, String, Map)} for details.
     */
    default void createShadowTopic(String shadowTopic, String sourceTopic) throws PulsarAdminException {
        createShadowTopic(shadowTopic, sourceTopic, null);
    }

    /**
     * Create a new shadow topic, see #{@link #createShadowTopic(String, String, Map)} for details.
     */
    default CompletableFuture<Void> createShadowTopicAsync(String shadowTopic, String sourceTopic) {
        return createShadowTopicAsync(shadowTopic, sourceTopic, null);
    }
}
