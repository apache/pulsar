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
package org.apache.pulsar.client.impl;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Provides lookup service to find broker which serves given topic. It helps to
 * lookup
 * <ul>
 * <li><b>topic-lookup:</b> lookup to find broker-address which serves given
 * topic</li>
 * <li><b>Partitioned-topic-Metadata-lookup:</b> lookup to find
 * PartitionedMetadata for a given topic</li>
 * </ul>
 *
 */
public interface LookupService extends AutoCloseable {

    /**
     * Instruct the LookupService to switch to a new service URL for all subsequent requests.
     */
    void updateServiceUrl(String serviceUrl) throws PulsarClientException;

    /**
     * Calls broker lookup-api to get broker {@link InetSocketAddress} which serves namespace bundle that contains given
     * topic.
     *
     * @param topicName
     *            topic-name
     * @return a {@link LookupTopicResult} representing the logical and physical address of the broker that serves the
     *         given topic, as well as proxying information.
     */
    CompletableFuture<LookupTopicResult> getBroker(TopicName topicName);

    /**
     * Returns {@link PartitionedTopicMetadata} for a given topic.
     * Note: this method will try to create the topic partitioned metadata if it does not exist.
     * @deprecated Please call {{@link #getPartitionedTopicMetadata(TopicName, boolean, boolean)}}.
     */
    @Deprecated
    default CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(TopicName topicName) {
        return getPartitionedTopicMetadata(topicName, true, true);
    }

    /**
     * See the doc {@link #getPartitionedTopicMetadata(TopicName, boolean, boolean)}.
     */
    default CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(TopicName topicName,
                                                                                boolean metadataAutoCreationEnabled) {
        return getPartitionedTopicMetadata(topicName, metadataAutoCreationEnabled, false);
    }

    /**
     * 1.Get the partitions if the topic exists. Return "{partition: n}" if a partitioned topic exists;
     *  return "{partition: 0}" if a non-partitioned topic exists.
     * 2. When {@param metadataAutoCreationEnabled} is "false," neither partitioned topic nor non-partitioned topic
     *   does not exist. You will get a {@link PulsarClientException.NotFoundException} or
     *   a {@link PulsarClientException.TopicDoesNotExistException}.
     *  2-1. You will get a {@link PulsarClientException.NotSupportedException} with metadataAutoCreationEnabled=false
     *       on an old broker version which does not support getting partitions without partitioned metadata
     *       auto-creation.
     * 3.When {@param metadataAutoCreationEnabled} is "true," it will trigger an auto-creation for this topic(using
     *  the default topic auto-creation strategy you set for the broker), and the corresponding result is returned.
     *  For the result, see case 1.
     * @param useFallbackForNonPIP344Brokers <p>If true, fallback to the prior behavior of the method
     *   {@link #getPartitionedTopicMetadata(TopicName)} if the broker does not support the PIP-344 feature
     *   'supports_get_partitioned_metadata_without_auto_creation'. This parameter only affects the behavior when
     *   {@param metadataAutoCreationEnabled} is false.</p>
     * @version 3.3.0.
     */
    CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(TopicName topicName,
                                                                        boolean metadataAutoCreationEnabled,
                                                                        boolean useFallbackForNonPIP344Brokers);

    /**
     * Returns current SchemaInfo {@link SchemaInfo} for a given topic.
     *
     * @param topicName topic-name
     * @return SchemaInfo
     */
    CompletableFuture<Optional<SchemaInfo>> getSchema(TopicName topicName);

    /**
     * Returns specific version SchemaInfo {@link SchemaInfo} for a given topic.
     *
     * @param topicName topic-name
     * @param version schema info version
     * @return SchemaInfo
     */
    CompletableFuture<Optional<SchemaInfo>> getSchema(TopicName topicName, byte[] version);

    /**
     * Returns broker-service lookup api url.
     *
     * @return
     */
    String getServiceUrl();

    /**
     * Resolves pulsar service url.
     *
     * @return the service url resolved to a socket address
     */
    InetSocketAddress resolveHost();

    /**
     * Returns all the topics that matches {@param topicPattern} for a given namespace.
     *
     * Note: {@param topicPattern} it relate to the topic name(without the partition suffix). For example:
     *  - There is a partitioned topic "tp-a" with two partitions.
     *    - tp-a-partition-0
     *    - tp-a-partition-1
     *  - If {@param topicPattern} is "tp-a", the consumer will subscribe to the two partitions.
     *  - if {@param topicPattern} is "tp-a-partition-0", the consumer will subscribe nothing.
     *
     * @param namespace : namespace-name
     * @return
     */
    CompletableFuture<GetTopicsResult> getTopicsUnderNamespace(NamespaceName namespace, Mode mode,
                                                               String topicPattern, String topicsHash);
}
