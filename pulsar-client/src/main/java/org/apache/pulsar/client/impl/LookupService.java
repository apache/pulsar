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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;

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
     *
     * @param topicName topic-name
     * @return
     */
    CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadata(TopicName topicName);

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
     * Returns all the topics name for a given namespace.
     *
     * @param namespace : namespace-name
     * @return
     */
    CompletableFuture<GetTopicsResult> getTopicsUnderNamespace(NamespaceName namespace, Mode mode,
                                                               String topicPattern, String topicsHash);

    /**
     * Get the exists partitions of a partitioned topic, the result does not contain the partitions which has not been
     * created yet(in other words, the partitions that do not exist in the response of "pulsar-admin topics list").
     * @return sorted partitions list if it is a partitioned topic; @return an empty list if it is a non-partitioned
     * topic.
     */
    default CompletableFuture<List<Integer>> getExistsPartitions(String topic) {
        TopicName topicName = TopicName.get(topic);
        if (!topicName.isPersistent()) {
            return FutureUtil.failedFuture(new IllegalArgumentException("The API LookupService.getExistsPartitions does"
                    + " not support non-persistent topic yet."));
        }
        return getTopicsUnderNamespace(topicName.getNamespaceObject(),
            topicName.isPersistent() ? Mode.PERSISTENT : Mode.NON_PERSISTENT,
            "^" + topicName.getPartitionedTopicName() + "$",
            null).thenApply(getTopicsResult -> {
                if (getTopicsResult.getNonPartitionedOrPartitionTopics() == null
                        || getTopicsResult.getNonPartitionedOrPartitionTopics().isEmpty()) {
                    return Collections.emptyList();
                }
                // If broker version is less than "2.11.x", it does not support broker-side pattern check, so append
                // a client-side pattern check.
                // If lookup service is typed HttpLookupService, the HTTP API does not support broker-side pattern
                // check yet, so append a client-side pattern check.
                Predicate<String> clientSideFilter;
                if (getTopicsResult.isFiltered()) {
                    clientSideFilter = __ -> true;
                } else {
                    clientSideFilter = tp -> Pattern.compile(TopicName.getPartitionPatten(topic)).matcher(tp).matches();
                }
                ArrayList<Integer> list = new ArrayList<>(getTopicsResult.getNonPartitionedOrPartitionTopics().size());
                for (String partition : getTopicsResult.getNonPartitionedOrPartitionTopics()) {
                    int partitionIndex = TopicName.get(partition).getPartitionIndex();
                    if (partitionIndex < 0) {
                        // It is not a partition.
                        continue;
                    }
                    if (clientSideFilter.test(partition)) {
                        list.add(partitionIndex);
                    }
                }
                Collections.sort(list);
                return list;
        });
    }
}
