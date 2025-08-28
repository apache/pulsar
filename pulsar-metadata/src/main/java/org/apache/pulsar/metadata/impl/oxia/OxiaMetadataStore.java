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
package org.apache.pulsar.metadata.impl.oxia;

import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.DeleteOption;
import io.oxia.client.api.Notification;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.PutOption;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataEventSynchronizer;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;

@Slf4j
public class OxiaMetadataStore extends AbstractMetadataStore {

    private final AsyncOxiaClient client;

    private final String identity;
    private Optional<MetadataEventSynchronizer> synchronizer;

    public OxiaMetadataStore(AsyncOxiaClient oxia, String identity) {
        super("oxia-metadata", OpenTelemetry.noop());
        this.client = oxia;
        this.identity = identity;
        this.synchronizer = Optional.empty();
        init();
    }

    public OxiaMetadataStore(
            @NonNull String serviceAddress,
            @NonNull String namespace,
            MetadataStoreConfig metadataStoreConfig,
            boolean enableSessionWatcher)
            throws Exception {
        super("oxia-metadata", Objects.requireNonNull(metadataStoreConfig).getOpenTelemetry());

        var linger = metadataStoreConfig.getBatchingMaxDelayMillis();
        if (!metadataStoreConfig.isBatchingEnabled()) {
            linger = 0;
        }
        synchronizer = Optional.ofNullable(metadataStoreConfig.getSynchronizer());
        identity = UUID.randomUUID().toString();
        OxiaClientBuilder oxiaClientBuilder = OxiaClientBuilder
                .create(serviceAddress)
                .clientIdentifier(identity)
                .namespace(namespace)
                .sessionTimeout(Duration.ofMillis(metadataStoreConfig.getSessionTimeoutMillis()))
                .batchLinger(Duration.ofMillis(linger))
                .maxRequestsPerBatch(metadataStoreConfig.getBatchingMaxOperations());
        if (StringUtils.isNotBlank(metadataStoreConfig.getConfigFilePath())) {
            oxiaClientBuilder.loadConfig(metadataStoreConfig.getConfigFilePath());
        }
        client = oxiaClientBuilder.asyncClient().get();
        init();
    }

    private void init() {
        updateMetadataEventSynchronizer(synchronizer.orElse(null));

        client.notifications(this::notificationCallback);
        super.registerSyncListener(synchronizer);
    }

    private void notificationCallback(Notification notification) {
        if (notification instanceof Notification.KeyCreated keyCreated) {
            receivedNotification(
                    new org.apache.pulsar.metadata.api.Notification(
                            NotificationType.Created, keyCreated.key()));
            notifyParentChildrenChanged(keyCreated.key());

        } else if (notification instanceof Notification.KeyModified keyModified) {
            receivedNotification(
                    new org.apache.pulsar.metadata.api.Notification(
                            NotificationType.Modified, keyModified.key()));
        } else if (notification instanceof Notification.KeyDeleted keyDeleted) {
            receivedNotification(
                    new org.apache.pulsar.metadata.api.Notification(
                            NotificationType.Deleted, keyDeleted.key()));
            notifyParentChildrenChanged(keyDeleted.key());
        } else {
            log.warn("Unknown notification type {}", notification);
        }
    }

    Optional<GetResult> convertGetResult(
            String path, io.oxia.client.api.GetResult result) {
        if (result == null) {
            return Optional.empty();
        }
        return Optional.of(result)
                .map(
                        oxiaResult ->
                                new GetResult(oxiaResult.getValue(), convertStat(path, oxiaResult.getVersion())));
    }

    Stat convertStat(String path, Version version) {
        return new Stat(
                path,
                version.versionId(),
                version.createdTimestamp(),
                version.modifiedTimestamp(),
                version.sessionId().isPresent(),
                version.clientIdentifier().stream().anyMatch(identity::equals),
                version.modificationsCount() == 0);
    }

    @Override
    public CompletableFuture<List<String>> getChildrenFromStore(String path) {
        var pathWithSlash = path + "/";

        return client
                .list(pathWithSlash, pathWithSlash + "/")
                .thenApply(
                        children ->
                                children.stream().map(child -> child.substring(pathWithSlash.length())).toList())
                .exceptionallyCompose(this::convertException);
    }

    @Override
    protected CompletableFuture<Boolean> existsFromStore(String path) {
        return client.get(path).thenApply(Objects::nonNull)
                .exceptionallyCompose(this::convertException);
    }

    @Override
    protected CompletableFuture<Optional<GetResult>> storeGet(String path) {
        return client.get(path).thenApply(res -> convertGetResult(path, res))
                .exceptionallyCompose(this::convertException);
    }

    @Override
    protected CompletableFuture<Void> storeDelete(String path, Optional<Long> expectedVersion) {
        return getChildrenFromStore(path)
                .thenCompose(
                        children -> {
                            if (!children.isEmpty()) {
                                return CompletableFuture.failedFuture(
                                        new MetadataStoreException("Key '" + path + "' has children"));
                            } else {
                                Set<DeleteOption> delOption =
                                        expectedVersion
                                                .map(v -> Collections.singleton(DeleteOption.IfVersionIdEquals(v)))
                                                .orElse(Collections.emptySet());
                                CompletableFuture<Boolean> result = client.delete(path, delOption);
                                return result
                                        .thenCompose(
                                                exists -> {
                                                    if (!exists) {
                                                        return CompletableFuture.failedFuture(
                                                                new MetadataStoreException.NotFoundException(
                                                                        "Key '" + path + "' does not exist"));
                                                    }
                                                    return CompletableFuture.completedFuture((Void) null);
                                                })
                                        .exceptionallyCompose(this::convertException);
                            }
                        });
    }

    @Override
    protected CompletableFuture<Stat> storePut(
            String path, byte[] data, Optional<Long> optExpectedVersion, EnumSet<CreateOption> options) {
        CompletableFuture<Void> parentsCreated = createParents(path);
        return parentsCreated.thenCompose(
                __ -> {
                    var expectedVersion = optExpectedVersion;
                    if (expectedVersion.isPresent()
                            && expectedVersion.get() != -1L
                            && options.contains(CreateOption.Sequential)) {
                        return CompletableFuture.failedFuture(
                                new MetadataStoreException(
                                        "Can't have expectedVersion and Sequential at the same time"));
                    }
                    CompletableFuture<String> actualPath;
                    if (options.contains(CreateOption.Sequential)) {
                        var parent = parent(path);
                        var parentPath = parent == null ? "/" : parent;

                        actualPath =
                                client
                                        .put(parentPath, new byte[] {})
                                        .thenApply(
                                                r -> String.format("%s%010d", path, r.version().modificationsCount()));
                        expectedVersion = Optional.of(-1L);
                    } else {
                        actualPath = CompletableFuture.completedFuture(path);
                    }
                    Set<PutOption> putOptions = new HashSet<>();
                    expectedVersion
                            .map(
                                    ver -> {
                                        if (ver == -1) {
                                            return PutOption.IfRecordDoesNotExist;
                                        }
                                        return PutOption.IfVersionIdEquals(ver);
                                    })
                            .ifPresent(putOptions::add);

                    if (options.contains(CreateOption.Ephemeral)) {
                        putOptions.add(PutOption.AsEphemeralRecord);
                    }
                    return actualPath
                            .thenCompose(
                                    aPath ->
                                            client
                                                    .put(aPath, data, putOptions)
                                                    .thenApply(res -> new PathWithPutResult(aPath, res)))
                            .thenApply(res -> convertStat(res.path(), res.result().version()))
                            .exceptionallyCompose(this::convertException);
                });
    }

    private <T> CompletionStage<T> convertException(Throwable ex) {
        Throwable actEx = FutureUtil.unwrapCompletionException(ex);
        if (actEx instanceof UnexpectedVersionIdException || actEx instanceof KeyAlreadyExistsException) {
            return CompletableFuture.failedFuture(
                    new MetadataStoreException.BadVersionException(actEx));
        } else if (actEx instanceof IllegalStateException) {
            return CompletableFuture.failedFuture(new MetadataStoreException.AlreadyClosedException(actEx));
        } else if (actEx instanceof MetadataStoreException) {
            return CompletableFuture.failedFuture(actEx);
        } else {
            return CompletableFuture.failedFuture(new MetadataStoreException(actEx));
        }
    }

    private static final byte[] EMPTY_VALUE = new byte[0];
    private static final Set<PutOption> IF_RECORD_DOES_NOT_EXIST =
            Collections.singleton(PutOption.IfRecordDoesNotExist);

    private CompletableFuture<Void> createParents(String path) {
        var parent = parent(path);
        if (parent == null || parent.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        return exists(parent)
                .thenCompose(
                        exists -> {
                            if (exists) {
                                return CompletableFuture.completedFuture(null);
                            } else {
                                return client
                                        .put(parent, EMPTY_VALUE, IF_RECORD_DOES_NOT_EXIST)
                                        .thenCompose(__ -> createParents(parent));
                            }
                        })
                .exceptionallyCompose(
                        ex -> {
                            if (ex.getCause() instanceof KeyAlreadyExistsException) {
                                return CompletableFuture.completedFuture(null);
                            }
                            return CompletableFuture.failedFuture(ex.getCause());
                        });
    }

    @Override
    public void close() throws Exception {
        if (isClosed.compareAndSet(false, true)) {
            if (client != null) {
                client.close();
            }
            super.close();
        }
    }

    public Optional<MetadataEventSynchronizer> getMetadataEventSynchronizer() {
        return synchronizer;
    }

    @Override
    public void updateMetadataEventSynchronizer(MetadataEventSynchronizer synchronizer) {
        this.synchronizer = Optional.ofNullable(synchronizer);
        registerSyncListener(this.synchronizer);
    }

    private record PathWithPutResult(String path, PutResult result) {}
}
