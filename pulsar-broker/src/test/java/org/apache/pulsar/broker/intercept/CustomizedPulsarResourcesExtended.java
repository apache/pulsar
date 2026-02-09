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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.pulsar.broker.DefaultPulsarResourcesExtended;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.jspecify.annotations.Nullable;

@Slf4j
public class CustomizedPulsarResourcesExtended extends DefaultPulsarResourcesExtended {

    private boolean enabledTopicWatcher;

    // Map<Namespace, Map<Topic, Map<PropertyKey, PropertyValue>>>
    private Map<String, Map<String, Map<String, String>>> customTopicPropertiesMap;

    @Override
    public CompletableFuture<List<String>> listTopicOfNamespace(NamespaceName namespaceName,
                                                                CommandGetTopicsOfNamespace.Mode mode,
                                                                @Nullable Map<String, String> properties) {
        if (MapUtils.isEmpty(properties)) {
            return super.listTopicOfNamespace(namespaceName, mode, properties);
        }

        if (enabledTopicWatcher) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Customized topic listing with properties is not supported when broker topic watcher is enabled."));
        }

        List<String> list = queryTopicListByProperties(namespaceName.toString(), properties);

        log.info("Customized topic listing in namespace: {}, mode: {}, properties: {}, result: {}",
                namespaceName, mode, properties, list);
        return CompletableFuture.completedFuture(list);
    }

    public List<String> queryTopicListByProperties(String namespace, Map<String, String> properties) {
        List<String> result = new ArrayList<>();
        Map<String, Map<String, String>> topicMap = this.customTopicPropertiesMap.get(namespace);
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
        return result;
    }

    public void setCustomProperties(TopicName topicName, Map<String, String> properties)
        throws ExecutionException, InterruptedException, TimeoutException {
        int timeoutSeconds = getPulsarService().getConfiguration().getMetadataStoreOperationTimeoutSeconds();
        PartitionedTopicMetadata partitionedTopicMetadata =
            getPulsarService().getBrokerService().fetchPartitionedTopicMetadataAsync(topicName)
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

    @Override
    public void initialize(PulsarService pulsarService) {
        super.initialize(pulsarService);
        this.enabledTopicWatcher = pulsarService.getConfiguration().isEnableBrokerTopicListWatcher();
        this.customTopicPropertiesMap = new ConcurrentHashMap<>();
    }

    @Override
    public void close() {
        this.customTopicPropertiesMap.clear();
    }
}
