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
package org.apache.pulsar.broker.intercept;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.TopicListingResult;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.topics.TopicsPattern;

@Slf4j
public class CustomizedTopicListingInterceptor implements BrokerInterceptor {

    private PulsarService pulsar;

    // Map<Namespace, Map<Topic, Map<PropertyKey, PropertyValue>>>
    private Map<String, Map<String, Map<String, String>>> customTopicPropertiesMap;
    private TopicsPattern.RegexImplementation topicsPatternImplementation;
    private boolean enableSubscriptionPatternEvaluation;
    private int maxSubscriptionPatternLength;
    private boolean enableTopicListWatcher;


    public void setCustomProperties(TopicName topicName, Map<String, String> properties)
        throws ExecutionException, InterruptedException, TimeoutException {
        int timeoutSeconds = pulsar.getConfiguration().getMetadataStoreOperationTimeoutSeconds();
        PartitionedTopicMetadata partitionedTopicMetadata =
            pulsar.getBrokerService().fetchPartitionedTopicMetadataAsync(topicName)
                .get(timeoutSeconds, TimeUnit.SECONDS);
        if (partitionedTopicMetadata.partitions == 0) {
            setNonPartitionedTopicCustomProperties(topicName, properties);
        } else {
            for (int i = 0; i < partitionedTopicMetadata.partitions; i++) {
                TopicName partitionedTopic = topicName.getPartition(i);
                setNonPartitionedTopicCustomProperties(partitionedTopic, properties);
            }
        }
    }

    private void setNonPartitionedTopicCustomProperties(TopicName topicName, Map<String, String> properties) {
        this.customTopicPropertiesMap.compute(topicName.getNamespace(), (ns, topicMap) -> {
            if (topicMap == null) {
                topicMap = new ConcurrentHashMap<>();
            }
            topicMap.put(topicName.toString(), properties);
            return topicMap;
        });
    }

    public List<String> queryTopicListByProperties(String namespace, Map<String, String> properties) {
        List<String> result = new ArrayList<>();
        Map<String, Map<String, String>> topicMap = this.customTopicPropertiesMap.get(namespace);
        if (topicMap != null) {
            for (Map.Entry<String, Map<String, String>> entry : topicMap.entrySet()) {
                String topic = entry.getKey();
                Map<String, String> topicProperties = entry.getValue();
                boolean match = true;
                for (Map.Entry<String, String> propEntry : properties.entrySet()) {
                    String key = propEntry.getKey();
                    String value = propEntry.getValue();
                    if (!value.equals(topicProperties.get(key))) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    result.add(topic);
                }
            }
        }
        return result;
    }

    @Override
    public void onPulsarCommand(BaseCommand command, ServerCnx cnx) throws InterceptException {

    }

    @Override
    public void onConnectionClosed(ServerCnx cnx) {

    }

    @Override
    public void onWebserviceRequest(ServletRequest request) throws IOException, ServletException, InterceptException {

    }

    @Override
    public void onWebserviceResponse(ServletRequest request, ServletResponse response)
        throws IOException, ServletException {

    }

    @Override
    public void initialize(PulsarService pulsarService) throws Exception {
        this.pulsar = pulsarService;
        this.customTopicPropertiesMap = new ConcurrentHashMap<>();
        ServiceConfiguration config = pulsarService.getConfig();
        this.topicsPatternImplementation = config.getTopicsPatternRegexImplementation();
        this.enableSubscriptionPatternEvaluation = config.isEnableBrokerSideSubscriptionPatternEvaluation();
        this.maxSubscriptionPatternLength = config.getSubscriptionPatternMaxLength();
        this.enableTopicListWatcher = config.isEnableBrokerTopicListWatcher();
    }

    @Override
    public CompletableFuture<Optional<TopicListingResult>> interceptGetTopicsOfNamespace(NamespaceName namespace,
                                                                                 CommandGetTopicsOfNamespace.Mode mode,
                                                                                 Optional<String> topicsPattern,
                                                                                 Map<String, String> properties) {
        if (enableTopicListWatcher) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "CustomizedTopicListingInterceptor does not support topic list watcher feature."));
        }

        List<String> list = queryTopicListByProperties(namespace.toString(), properties);
        List<String> filteredTopics = TopicList.filterSystemTopic(list);
        // Pattern Filtering (if applicable)
        if (enableSubscriptionPatternEvaluation && topicsPattern.isPresent()) {
            if (topicsPattern.get().length() <= maxSubscriptionPatternLength) {
                filteredTopics = TopicList.filterTopics(filteredTopics, topicsPattern.get(),
                    topicsPatternImplementation);
            } else {
                log.info("[{}] Subscription pattern provided [{}] was longer than maximum {}.",
                    namespace, topicsPattern.get(), maxSubscriptionPatternLength);
            }
        }
        log.info("[{}] Topic list intercepted [{}]", namespace, filteredTopics);
        return CompletableFuture.completedFuture(Optional.of(new TopicListingResult(filteredTopics, true)));
    }

    @Override
    public void close() {

    }
}
