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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.client.admin.NonPersistentTopics;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.naming.DestinationName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.NonPersistentTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicStats;

public class NonPersistentTopicsImpl extends BaseResource implements NonPersistentTopics {

    private final WebTarget persistentTopics;

    public NonPersistentTopicsImpl(WebTarget web, Authentication auth) {
        super(auth);
        this.persistentTopics = web.path("/non-persistent");
    }

    @Override
    public void createPartitionedTopic(String destination, int numPartitions) throws PulsarAdminException {
        try {
            createPartitionedTopicAsync(destination, numPartitions).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<Void> createPartitionedTopicAsync(String destination, int numPartitions) {
        checkArgument(numPartitions > 1, "Number of partitions should be more than 1");
        DestinationName ds = validateTopic(destination);
        return asyncPutRequest(
                persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("partitions"),
                Entity.entity(numPartitions, MediaType.APPLICATION_JSON));
    }

    @Override
    public PartitionedTopicMetadata getPartitionedTopicMetadata(String destination) throws PulsarAdminException {
        try {
            return getPartitionedTopicMetadataAsync(destination).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<PartitionedTopicMetadata> getPartitionedTopicMetadataAsync(String destination) {
        DestinationName ds = validateTopic(destination);
        final CompletableFuture<PartitionedTopicMetadata> future = new CompletableFuture<>();
        asyncGetRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("partitions"),
                new InvocationCallback<PartitionedTopicMetadata>() {

                    @Override
                    public void completed(PartitionedTopicMetadata response) {
                        future.complete(response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public NonPersistentTopicStats getStats(String destination) throws PulsarAdminException {
        try {
            return getStatsAsync(destination).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<NonPersistentTopicStats> getStatsAsync(String destination) {
        DestinationName ds = validateTopic(destination);
        final CompletableFuture<NonPersistentTopicStats> future = new CompletableFuture<>();
        asyncGetRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("stats"),
                new InvocationCallback<NonPersistentTopicStats>() {

                    @Override
                    public void completed(NonPersistentTopicStats response) {
                        future.complete(response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public PersistentTopicInternalStats getInternalStats(String destination) throws PulsarAdminException {
        try {
            return getInternalStatsAsync(destination).get();
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e.getCause());
        }
    }

    @Override
    public CompletableFuture<PersistentTopicInternalStats> getInternalStatsAsync(String destination) {
        DestinationName ds = validateTopic(destination);
        final CompletableFuture<PersistentTopicInternalStats> future = new CompletableFuture<>();
        asyncGetRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("internalStats"),
                new InvocationCallback<PersistentTopicInternalStats>() {

                    @Override
                    public void completed(PersistentTopicInternalStats response) {
                        future.complete(response);
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    /*
     * returns destination name with encoded Local Name
     */
    private DestinationName validateTopic(String destination) {
        // Parsing will throw exception if name is not valid
        return DestinationName.get(destination);
    }

}
