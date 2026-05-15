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
import io.oxia.client.api.Notification;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.GetSequenceUpdatesOption;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.RangeScanOption;
import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Predicate;
import lombok.CustomLog;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataEventSynchronizer;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.OptionsHelper;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;

@CustomLog
public class OxiaMetadataStore extends AbstractMetadataStore {

    private final AsyncOxiaClient client;

    private final String identity;
    private Optional<MetadataEventSynchronizer> synchronizer;

    public OxiaMetadataStore(AsyncOxiaClient oxia, String identity) {
        super("oxia-metadata", OpenTelemetry.noop(), null, 1);
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
        super("oxia-metadata", Objects.requireNonNull(metadataStoreConfig).getOpenTelemetry(),
                metadataStoreConfig.getNodeSizeStats(), metadataStoreConfig.getNumSerDesThreads());

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
            log.warn().attr("notification", notification).log("Unknown notification type");
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
                                new GetResult(oxiaResult.value(), convertStat(path, oxiaResult.version())));
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
    public CompletableFuture<List<String>> getChildrenFromStore(String path, Set<Option> opts) {
        var pathWithSlash = path.endsWith("/") ? path : path + "/";

        return client
                .list(pathWithSlash, pathWithSlash + "/", listOptions(opts))
                .thenApply(
                        children ->
                                children.stream().map(child -> child.substring(pathWithSlash.length())).toList())
                .exceptionallyCompose(this::convertException);
    }

    @Override
    protected CompletableFuture<Boolean> existsFromStore(String path, Set<Option> opts) {
        return client.get(path, getOptions(opts)).thenApply(Objects::nonNull)
                .exceptionallyCompose(this::convertException);
    }

    @Override
    protected CompletableFuture<Optional<GetResult>> storeGet(String path, Set<Option> opts) {
        return client.get(path, getOptions(opts)).thenApply(res -> convertGetResult(path, res))
                .exceptionallyCompose(this::convertException);
    }

    @Override
    protected CompletableFuture<Void> storeDelete(String path, Optional<Long> expectedVersion, Set<Option> opts) {
        return getChildrenFromStore(path, opts)
                .thenCompose(
                        children -> {
                            if (!children.isEmpty()) {
                                return CompletableFuture.failedFuture(
                                        new MetadataStoreException("Key '" + path + "' has children"));
                            } else {
                                Set<DeleteOption> delOption = deleteOptions(opts, expectedVersion);
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
            String path, byte[] data, Optional<Long> optExpectedVersion, Set<Option> opts) {
        return doStorePut(path, data, optExpectedVersion, opts);
    }

    @Override
    protected CompletableFuture<Void> storeScanChildren(String parentPath, ScanConsumer consumer, Set<Option> opts) {
        // Oxia's hierarchical sort makes [parentPath + "/", parentPath + "//") the canonical
        // range covering exactly the immediate children — same convention getChildrenFromStore
        // uses with `client.list(...)`.
        String firstKey = parentPath.endsWith("/") ? parentPath : parentPath + "/";
        String lastKey = firstKey + "/";
        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            client.rangeScan(firstKey, lastKey, new io.oxia.client.api.RangeScanConsumer() {
                @Override
                public void onNext(io.oxia.client.api.GetResult result) {
                    consumer.onNext(new GetResult(result.value(),
                            convertStat(result.key(), result.version())));
                }

                @Override
                public void onError(Throwable throwable) {
                    consumer.onError(throwable);
                    done.completeExceptionally(throwable);
                }

                @Override
                public void onCompleted() {
                    consumer.onCompleted();
                    done.complete(null);
                }
            }, rangeScanOptions(opts));
        } catch (Throwable t) {
            consumer.onError(t);
            done.completeExceptionally(t);
        }
        return done;
    }

    @Override
    protected CompletableFuture<Void> storeScanByIndex(
            String scanPathPrefix, String indexName,
            String fromKeyInclusive, String toKeyInclusive,
            Predicate<GetResult> fallbackFilter,
            ScanConsumer consumer, Set<Option> opts) {
        // Index entries are stored at "<parentPath>/<secondaryKey>" by doStorePut, where
        // parentPath == scanPathPrefix for records directly under the prefix. The API uses
        // inclusive-on-both-sides bounds; Oxia's list(start, end, ...) is half-open, so we
        // append a sentinel character to the upper bound to widen the half-open range to
        // include `toKeyInclusive`. We use '~' (0x7E) — it lex-sorts after every printable
        // ASCII byte, which is a safe choice for the secondary keys callers actually use today
        // (numeric-encoded timestamps, fixed-tag enums, etc.). Callers that need full byte-range
        // coverage can pass `toKeyInclusive=null` or use a different scheme.
        String scopedFrom = fromKeyInclusive == null
                ? scanPathPrefix + "/"
                : scanPathPrefix + "/" + fromKeyInclusive;
        String scopedTo = toKeyInclusive == null
                ? scanPathPrefix + "0"   // '0' (0x30) is the lex successor of '/' (0x2F)
                : scanPathPrefix + "/" + toKeyInclusive + "~";
        Set<ListOption> listOpts = new HashSet<>(listOptions(opts));
        listOpts.add(ListOption.UseIndex(indexName));
        CompletableFuture<Void> done = new CompletableFuture<>();
        client.list(scopedFrom, scopedTo, listOpts)
                .thenCompose(primaryKeys -> {
                    // Native indexes already enforced the range; fallbackFilter is unused here
                    // (it's the scan-and-filter compat-path predicate).
                    CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);
                    for (String key : primaryKeys) {
                        chain = chain
                                .thenCompose(__ -> storeGet(key, opts))
                                .thenAccept(opt -> opt.ifPresent(consumer::onNext));
                    }
                    return chain;
                })
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        Throwable cause = FutureUtil.unwrapCompletionException(ex);
                        consumer.onError(cause);
                        done.completeExceptionally(cause);
                    } else {
                        consumer.onCompleted();
                        done.complete(null);
                    }
                });
        return done;
    }

    private CompletableFuture<Stat> doStorePut(
            String path, byte[] data, Optional<Long> optExpectedVersion, Set<Option> opts) {
        boolean sequential = OptionsHelper.isSequential(opts);
        boolean ephemeral = OptionsHelper.isEphemeral(opts);
        List<Long> sequenceKeysDeltas = OptionsHelper.sequenceKeysDeltas(opts);
        Map<String, String> secondaryIndexes = OptionsHelper.secondaryIndexes(opts);
        if (sequential && sequenceKeysDeltas != null) {
            return CompletableFuture.failedFuture(new MetadataStoreException(
                    "Sequential and SequenceKeysDeltas cannot be combined"));
        }
        CompletableFuture<Void> parentsCreated = createParents(path);
        return parentsCreated.thenCompose(
                __ -> {
                    var expectedVersion = optExpectedVersion;
                    if (expectedVersion.isPresent() && expectedVersion.get() != -1L
                            && (sequential || sequenceKeysDeltas != null)) {
                        return CompletableFuture.failedFuture(
                                new MetadataStoreException(
                                        "Can't have expectedVersion and Sequential/SequenceKeysDeltas at the "
                                                + "same time"));
                    }
                    CompletableFuture<String> actualPath;
                    if (sequential) {
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

                    if (ephemeral) {
                        putOptions.add(PutOption.AsEphemeralRecord);
                    }
                    String partitionKey = OptionsHelper.partitionKey(opts);
                    if (partitionKey != null) {
                        putOptions.add(PutOption.PartitionKey(partitionKey));
                    }
                    if (sequenceKeysDeltas != null) {
                        putOptions.add(PutOption.SequenceKeysDeltas(sequenceKeysDeltas));
                    }
                    var parentPath = parent(path);
                    var parentPrefix = parentPath == null ? "" : parentPath;
                    secondaryIndexes.forEach((indexName, secondaryKey) ->
                            putOptions.add(PutOption.SecondaryIndex(indexName,
                                    parentPrefix + "/" + secondaryKey)));

                    return actualPath
                            .thenCompose(
                                    aPath ->
                                            client
                                                    .put(aPath, data, putOptions)
                                                    .thenApply(res -> new PathWithPutResult(aPath, res)))
                            // Use the effective key returned by Oxia — for SequenceKeysDeltas this is
                            // the server-assigned key with sequence suffixes appended.
                            .thenApply(res -> convertStat(res.result().key(), res.result().version()))
                            .exceptionallyCompose(this::convertException);
                });
    }

    /** Build the Oxia {@link GetOption} set from {@code opts}, currently routing the partition key. */
    private static Set<GetOption> getOptions(Set<Option> opts) {
        String partitionKey = OptionsHelper.partitionKey(opts);
        return partitionKey == null ? Set.of() : Set.of(GetOption.PartitionKey(partitionKey));
    }

    /** Build the Oxia {@link ListOption} set from {@code opts}, currently routing the partition key. */
    private static Set<ListOption> listOptions(Set<Option> opts) {
        String partitionKey = OptionsHelper.partitionKey(opts);
        return partitionKey == null ? Set.of() : Set.of(ListOption.PartitionKey(partitionKey));
    }

    /**
     * Build the Oxia {@link DeleteOption} set from {@code opts} together with the optional expected
     * version.
     */
    private static Set<DeleteOption> deleteOptions(Set<Option> opts, Optional<Long> expectedVersion) {
        String partitionKey = OptionsHelper.partitionKey(opts);
        if (partitionKey == null && expectedVersion.isEmpty()) {
            return Set.of();
        }
        Set<DeleteOption> result = new HashSet<>();
        expectedVersion.ifPresent(v -> result.add(DeleteOption.IfVersionIdEquals(v)));
        if (partitionKey != null) {
            result.add(DeleteOption.PartitionKey(partitionKey));
        }
        return result;
    }

    /** Build the Oxia {@link RangeScanOption} set from {@code opts}, currently routing the partition key. */
    private static Set<RangeScanOption> rangeScanOptions(Set<Option> opts) {
        String partitionKey = OptionsHelper.partitionKey(opts);
        return partitionKey == null ? Set.of() : Set.of(RangeScanOption.PartitionKey(partitionKey));
    }

    /**
     * Build the Oxia {@link GetSequenceUpdatesOption} set from {@code opts}, currently routing the
     * partition key.
     */
    private static Set<GetSequenceUpdatesOption> sequenceUpdatesOptions(Set<Option> opts) {
        String partitionKey = OptionsHelper.partitionKey(opts);
        return partitionKey == null
                ? Set.of()
                : Set.of(GetSequenceUpdatesOption.PartitionKey(partitionKey));
    }

    @Override
    public AutoCloseable subscribeSequence(String prefix, Consumer<String> listener, Set<Option> opts) {
        Closeable handle = client.getSequenceUpdates(prefix, listener, sequenceUpdatesOptions(opts));
        return handle::close;
    }

    @Override
    protected boolean supportsNativeSequenceKeys() {
        return true;
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
