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
package org.apache.pulsar.client.admin.internal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.client.WebTarget;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.Lookup;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

public class LookupImpl extends BaseResource implements Lookup {

    private final WebTarget v2lookup;
    private final boolean useTls;
    private final Topics topics;

    public LookupImpl(WebTarget web, Authentication auth, boolean useTls, long readTimeoutMs, Topics topics) {
        super(auth, readTimeoutMs);
        this.useTls = useTls;
        v2lookup = web.path("/lookup/v2");
        this.topics = topics;
    }

    @Override
    public String lookupTopic(String topic) throws PulsarAdminException {
        return sync(() -> lookupTopicAsync(topic));
    }

    @Override
    public CompletableFuture<String> lookupTopicAsync(String topic) {
        TopicName topicName = TopicName.get(topic);
        String prefix = topicName.isV2() ? "/topic" : "/destination";
        WebTarget path = v2lookup.path(prefix).path(topicName.getLookupName());

        return asyncGetRequest(path, new FutureCallback<LookupData>() {})
                .thenApply(lookupData -> useTls && StringUtils.isNotBlank(lookupData.getBrokerUrlTls())
                        ? lookupData.getBrokerUrlTls() : lookupData.getBrokerUrl());
    }

    @Override
    public Map<String, String> lookupPartitionedTopic(String topic) throws PulsarAdminException {
        return sync(() -> lookupPartitionedTopicAsync(topic));
    }

    @Override
    public CompletableFuture<Map<String, String>> lookupPartitionedTopicAsync(String topic) {
        CompletableFuture<Map<String, String>> future = new CompletableFuture<>();
        topics.getPartitionedTopicMetadataAsync(topic).thenAccept(partitionedTopicMetadata -> {
            int partitions = partitionedTopicMetadata.partitions;
            if (partitions <= 0) {
               future.completeExceptionally(
                        new PulsarAdminException("Topic " + topic + " is not a partitioned topic"));
               return;
            }

            Map<String, CompletableFuture<String>> lookupResult = new LinkedHashMap<>(partitions);
            for (int i = 0; i < partitions; i++) {
                String partitionTopicName = topic + "-partition-" + i;
                lookupResult.put(partitionTopicName, lookupTopicAsync(partitionTopicName));
            }

            FutureUtil.waitForAll(new ArrayList<>(lookupResult.values())).whenComplete((url, throwable) ->{
               if (throwable != null) {
                   future.completeExceptionally(getApiException(throwable.getCause()));
                   return;
               }
               Map<String, String> result = new LinkedHashMap<>();
               for (Map.Entry<String, CompletableFuture<String>> entry : lookupResult.entrySet()) {
                   try {
                       result.put(entry.getKey(), entry.getValue().get());
                   } catch (InterruptedException | ExecutionException e) {
                       future.completeExceptionally(e);
                       return;
                   }
               }
               future.complete(result);
            });

        }).exceptionally(throwable -> {
            future.completeExceptionally(getApiException(throwable.getCause()));
            return null;
        });

        return future;
    }

    @Override
    public String getBundleRange(String topic) throws PulsarAdminException {
        return sync(() -> getBundleRangeAsync(topic));
    }

    @Override
    public CompletableFuture<String> getBundleRangeAsync(String topic) {
        TopicName topicName = TopicName.get(topic);
        String prefix = topicName.isV2() ? "/topic" : "/destination";
        WebTarget path = v2lookup.path(prefix).path(topicName.getLookupName()).path("bundle");
        return asyncGetRequest(path, new FutureCallback<String>(){});
    }

}
