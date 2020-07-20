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
package org.apache.pulsar.client.admin.internal;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;

import org.apache.pulsar.client.admin.Lookup;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;

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
        try {
            return lookupTopicAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<String> lookupTopicAsync(String topic) {
        TopicName topicName = TopicName.get(topic);
        String prefix = topicName.isV2() ? "/topic" : "/destination";
        WebTarget path = v2lookup.path(prefix).path(topicName.getLookupName());

        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<LookupData>() {
                    @Override
                    public void completed(LookupData lookupData) {
                        if (useTls) {
                            future.complete(lookupData.getBrokerUrlTls());
                        } else {
                            future.complete(lookupData.getBrokerUrl());
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public Map<String, String> lookupPartitionedTopic(String topic) throws PulsarAdminException {
        try {
            PartitionedTopicMetadata partitionedTopicMetadata =
                    topics.getPartitionedTopicMetadataAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
            int partitions = partitionedTopicMetadata.partitions;
            if (partitions <= 0) {
                throw new ExecutionException(
                        new PulsarAdminException("Topic " + topic + " is not a partitioned topic"));
            }

            Map<String, String> lookupResult = new LinkedHashMap<>(partitions);
            for (int i = 0; i < partitions; i ++) {
                String partitionTopicName = topic + "-partition-" + i;
                lookupResult.put(partitionTopicName,
                        lookupTopicAsync(partitionTopicName).get(readTimeoutMs, TimeUnit.MILLISECONDS));
            }
            return lookupResult;
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }


    @Override
    public String getBundleRange(String topic) throws PulsarAdminException {
        try {
            return getBundleRangeAsync(topic).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<String> getBundleRangeAsync(String topic) {
        TopicName topicName = TopicName.get(topic);
        String prefix = topicName.isV2() ? "/topic" : "/destination";
        WebTarget path = v2lookup.path(prefix).path(topicName.getLookupName()).path("bundle");
        final CompletableFuture<String> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<String>() {
                    @Override
                    public void completed(String bundleRange) {
                        future.complete(bundleRange);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

}
