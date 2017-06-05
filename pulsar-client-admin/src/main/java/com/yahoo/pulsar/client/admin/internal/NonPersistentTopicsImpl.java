/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.admin.internal;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.ServerErrorException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.yahoo.pulsar.client.admin.NonPersistentTopics;
import com.yahoo.pulsar.client.admin.PulsarAdminException;
import com.yahoo.pulsar.client.admin.PulsarAdminException.NotFoundException;
import com.yahoo.pulsar.client.api.Authentication;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.impl.MessageImpl;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.NamespaceName;
import com.yahoo.pulsar.common.partition.PartitionedTopicMetadata;
import com.yahoo.pulsar.common.policies.data.AuthAction;
import com.yahoo.pulsar.common.policies.data.ErrorData;
import com.yahoo.pulsar.common.policies.data.PartitionedTopicStats;
import com.yahoo.pulsar.common.policies.data.PersistentTopicInternalStats;
import com.yahoo.pulsar.common.policies.data.PersistentTopicStats;
import com.yahoo.pulsar.common.util.Codec;

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
    public PersistentTopicStats getStats(String destination) throws PulsarAdminException {
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
    public CompletableFuture<PersistentTopicStats> getStatsAsync(String destination) {
        DestinationName ds = validateTopic(destination);
        final CompletableFuture<PersistentTopicStats> future = new CompletableFuture<>();
        asyncGetRequest(persistentTopics.path(ds.getNamespace()).path(ds.getEncodedLocalName()).path("stats"),
                new InvocationCallback<PersistentTopicStats>() {

                    @Override
                    public void completed(PersistentTopicStats response) {
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
