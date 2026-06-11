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
package org.apache.pulsar.metadata.api;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.Beta;
import io.github.merlimat.slog.Logger;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;

/**
 * Metadata store client interface.
 *
 * NOTE: This API is still evolving and will be refactored as needed until all the metadata usages are converted into
 * using it.
 */
@Beta
public interface MetadataStore extends AutoCloseable {

    Logger LOG = Logger.get(MetadataStore.class);

    /**
     * Read the value of one key, identified by the path, with a set of {@link Option options}, e.g.
     * {@link Option.PartitionKey} for routing on sharded backends. Pass {@link Set#of()} for no options.
     *
     * The async call will return a future that yields a {@link GetResult} that will contain the value and the
     * associated {@link Stat} object.
     *
     * If the value is not found, the future will yield an empty {@link Optional}.
     *
     * @param path
     *            the path of the key to get from the store
     * @param opts
     *            the set of {@link Option options} for this operation
     * @return a future to track the async request
     */
    CompletableFuture<Optional<GetResult>> get(String path, Set<Option> opts);

    /**
     * Like {@link #get(String, Set)} with no options.
     */
    default CompletableFuture<Optional<GetResult>> get(String path) {
        return get(path, Set.of());
    }


    /**
     * Ensure that the next value read from  the local client will be up-to-date with the latest version of the value
     * as it can be seen by all the other clients.
     * @param path
     * @return a handle to the operation
     */
    default CompletableFuture<Void> sync(String path) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Return all the nodes (lexicographically sorted) that are children to the specific path.
     *
     * If the path itself does not exist, it will return an empty list.
     *
     * @param path
     *            the path of the key to get from the store
     * @param opts
     *            the set of {@link Option options} for this operation
     * @return a future to track the async request
     */
    CompletableFuture<List<String>> getChildren(String path, Set<Option> opts);

    /**
     * Like {@link #getChildren(String, Set)} with no options.
     */
    default CompletableFuture<List<String>> getChildren(String path) {
        return getChildren(path, Set.of());
    }


    /**
     * Return all the nodes (lexicographically sorted) that are children to the specific path.
     *
     * If the path itself does not exist, it will return an empty list.
     *
     * This method is similar to {@link #getChildren(String, Set)}, but it attempts to read directly from
     *  the underlying store.
     *
     * @param path
     *            the path of the key to get from the store
     * @param opts
     *            the set of {@link Option options} for this operation
     * @return a future to track the async request
     */
    CompletableFuture<List<String>> getChildrenFromStore(String path, Set<Option> opts);

    /**
     * Like {@link #getChildrenFromStore(String, Set)} with no options.
     */
    default CompletableFuture<List<String>> getChildrenFromStore(String path) {
        return getChildrenFromStore(path, Set.of());
    }

    /**
     * Read whether a specific path exists.
     *
     * Note: In case of keys with multiple levels (eg: '/a/b/c'), checking the existence of a parent (eg. '/a') might
     * not necessarily return true, unless the key had been explicitly created.
     *
     * @param path
     *            the path of the key to check on the store
     * @param opts
     *            the set of {@link Option options} for this operation
     * @return a future to track the async request
     */
    CompletableFuture<Boolean> exists(String path, Set<Option> opts);

    /**
     * Like {@link #exists(String, Set)} with no options.
     */
    default CompletableFuture<Boolean> exists(String path) {
        return exists(path, Set.of());
    }

    /**
     * Put a new value for a given key with a set of {@link Option options}, e.g.
     * {@link Option.Ephemeral}, {@link Option.Sequential}, {@link Option.SecondaryIndex}, or
     * {@link Option.PartitionKey}. Pass {@link Set#of()} for no options.
     *
     * The caller can specify an expected version to be atomically checked against the current version of the stored
     * data.
     *
     * The future will return the {@link Stat} object associated with the newly inserted value.
     *
     * @param path
     *            the path of the key to delete from the store
     * @param value
     *            the value to
     * @param expectedVersion
     *            if present, the version will have to match with the currently stored value for the operation to
     *            succeed. Use -1 to enforce a non-existing value.
     * @param opts
     *            the set of {@link Option options} for this operation
     * @throws BadVersionException
     *             if the expected version doesn't match the actual version of the data
     * @return a future to track the async request
     */
    CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion, Set<Option> opts);

    /**
     * Like {@link #put(String, byte[], Optional, Set)} with no options.
     */
    default CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion) {
        return put(path, value, expectedVersion, Set.of());
    }

    /**
     *
     * @param path
     *            the path of the key to delete from the store
     * @param expectedVersion
     *            if present, the version will have to match with the currently stored value for the operation to
     *            succeed
     * @param opts
     *            the set of {@link Option options} for this operation
     * @throws NotFoundException
     *             if the path is not found
     * @throws BadVersionException
     *             if the expected version doesn't match the actual version of the data
     * @return a future to track the async request
     */
    CompletableFuture<Void> delete(String path, Optional<Long> expectedVersion, Set<Option> opts);

    /**
     * Like {@link #delete(String, Optional, Set)} with no options.
     */
    default CompletableFuture<Void> delete(String path, Optional<Long> expectedVersion) {
        return delete(path, expectedVersion, Set.of());
    }

    default CompletableFuture<Void> deleteIfExists(String path, Optional<Long> expectedVersion, Set<Option> opts) {
        return delete(path, expectedVersion, opts)
                .exceptionally(e -> {
                    if (e.getCause() instanceof NotFoundException) {
                        LOG.info().attr("path", path).log("Path not found while deleting (this is not a problem)");
                        return null;
                    } else {
                        if (expectedVersion.isEmpty()) {
                            LOG.info().attr("path", path).exception(e).log("Failed to delete path");
                        } else {
                            LOG.info().attr("path", path).attr("expectedVersion", expectedVersion).exception(e)
                                    .log("Failed to delete path");
                        }
                        throw new CompletionException(e);
                    }
                });
    }

    /** Like {@link #deleteIfExists(String, Optional, Set)} with no options. */
    default CompletableFuture<Void> deleteIfExists(String path, Optional<Long> expectedVersion) {
        return deleteIfExists(path, expectedVersion, Set.of());
    }

    /**
     * Delete a key-value pair and all the children nodes.
     *
     * Note: the operation might not be carried in an atomic fashion. If the operation fails, the deletion of the
     *       tree might be only partial.
     *
     * @param path
     *            the path of the key to delete from the store
     * @param opts
     *            the set of {@link Option options} for this operation
     * @return a future to track the async request
     */
    CompletableFuture<Void> deleteRecursive(String path, Set<Option> opts);

    /** Like {@link #deleteRecursive(String, Set)} with no options. */
    default CompletableFuture<Void> deleteRecursive(String path) {
        return deleteRecursive(path, Set.of());
    }

    /**
     * Register a listener that will be called on changes in the underlying store.
     *
     * @param listener
     *            a consumer of notifications
     */
    void registerListener(Consumer<Notification> listener);

    /**
     * Create a metadata cache specialized for a specific class.
     *
     * @param <T>
     * @param clazz
     *            the class type to be used for serialization/deserialization
     * @param cacheConfig
     *          the cache configuration to be used
     * @return the metadata cache object
     */
    <T> MetadataCache<T> getMetadataCache(Class<T> clazz, MetadataCacheConfig<?> cacheConfig);

    /**
     * Create a metadata cache specialized for a specific class.
     *
     * @param <T>
     * @param clazz
     *            the class type to be used for serialization/deserialization
     * @return the metadata cache object
     */
    default <T> MetadataCache<T> getMetadataCache(Class<T> clazz) {
        return getMetadataCache(clazz, getDefaultMetadataCacheConfig());
    }

    /**
     * Create a metadata cache specialized for a specific class.
     *
     * @param <T>
     * @param typeRef
     *            the type ref description to be used for serialization/deserialization
     * @param cacheConfig
     *          the cache configuration to be used
     * @return the metadata cache object
     */
    <T> MetadataCache<T> getMetadataCache(TypeReference<T> typeRef, MetadataCacheConfig<?> cacheConfig);

    /**
     * Create a metadata cache specialized for a specific class.
     *
     * @param <T>
     * @param typeRef
     *            the type ref description to be used for serialization/deserialization
     * @return the metadata cache object
     */
    default <T> MetadataCache<T> getMetadataCache(TypeReference<T> typeRef) {
        return getMetadataCache(typeRef, getDefaultMetadataCacheConfig());
    }

    /**
     * Create a metadata cache that uses a particular serde object.
     *
     * @param <T>
     * @param serde
     *            the custom serialization/deserialization object
     * @param cacheConfig
     *          the cache configuration to be used
     * @deprecated use {@link #getMetadataCache(String, MetadataSerde, MetadataCacheConfig)}
     * @return the metadata cache object
     */
    @Deprecated
    default <T> MetadataCache<T> getMetadataCache(MetadataSerde<T> serde, MetadataCacheConfig<?> cacheConfig) {
        return getMetadataCache("serde", serde, cacheConfig);
    }

    /**
     * Create a metadata cache that uses a particular serde object.
     *
     * @param <T>
     * @param serde
     *            the custom serialization/deserialization object
     * @return the metadata cache object
     */
    default <T> MetadataCache<T> getMetadataCache(MetadataSerde<T> serde) {
        return getMetadataCache(serde, getDefaultMetadataCacheConfig());
    }

    /**
     * Create a metadata cache that uses a particular serde object.
     *
     * @param <T>
     * @param serde
     *            the custom serialization/deserialization object
     * @return the metadata cache object
     */
    default <T> MetadataCache<T> getMetadataCache(String cacheName, MetadataSerde<T> serde,
                                                  MetadataCacheConfig<?> cacheConfig) {
        return getMetadataCache(serde, cacheConfig);
    }

    /**
     * Stream records matching a secondary-index value or range.
     *
     * <p>One method serves both point lookup and range queries:
     * <ul>
     *   <li>Point lookup: {@code fromKey == toKey == key}.</li>
     *   <li>Range: pass {@code null} on either side for an unbounded bound, or specific values for
     *       a closed range. Both bounds are <b>inclusive</b>.</li>
     * </ul>
     *
     * <p>On stores that support secondary indexes natively (e.g. Oxia), this is a single store-side
     * range scan over the index. On stores that don't (e.g. ZooKeeper), it falls back to listing
     * all children under {@code scanPathPrefix}, fetching each record, and applying
     * {@code fallbackFilter}; the fallback also enforces the {@code [fromKey, toKey]} bounds.
     *
     * <p>Results are streamed to {@code consumer} as they become available — {@link ScanConsumer#onNext}
     * once per match, then exactly one of {@link ScanConsumer#onCompleted} or
     * {@link ScanConsumer#onError}. The returned future completes when the scan terminates and
     * mirrors the terminal callback.
     *
     * @param scanPathPrefix    path prefix scoping the scan (and used by the fallback list)
     * @param indexName         the secondary-index name
     * @param fromKeyInclusive  lower bound on the secondary-key value, or {@code null} for unbounded
     * @param toKeyInclusive    upper bound on the secondary-key value, or {@code null} for unbounded
     * @param fallbackFilter    additional predicate applied during fallback scan; ignored by native implementations
     * @param consumer          callback receiving records, completion, or an error
     * @param opts              the set of {@link Option options} for this operation
     * @return a future that completes when the scan terminates
     */
    default CompletableFuture<Void> scanByIndex(
            String scanPathPrefix, String indexName,
            String fromKeyInclusive, String toKeyInclusive,
            Predicate<GetResult> fallbackFilter,
            ScanConsumer consumer, Set<Option> opts) {
        MetadataStoreException ex =
                new MetadataStoreException("Secondary index queries not supported by this store");
        consumer.onError(ex);
        return CompletableFuture.failedFuture(ex);
    }

    /**
     * Like {@link #scanByIndex(String, String, String, String, Predicate, ScanConsumer, Set)} with no options.
     */
    default CompletableFuture<Void> scanByIndex(
            String scanPathPrefix, String indexName,
            String fromKeyInclusive, String toKeyInclusive,
            Predicate<GetResult> fallbackFilter,
            ScanConsumer consumer) {
        return scanByIndex(scanPathPrefix, indexName, fromKeyInclusive, toKeyInclusive,
                fallbackFilter, consumer, Set.of());
    }

    /**
     * Stream all direct children of {@code parentPath} together with their values.
     *
     * <p>This is the value-bearing counterpart to {@link #getChildren} — same semantics for
     * what counts as a "child" (one hierarchical level below {@code parentPath}, no
     * descendants), but each record carries the value and {@link Stat} alongside the path.
     * Results are delivered to {@code consumer} as they become available so callers don't
     * have to materialize a potentially-large list in memory.
     *
     * <p>The consumer's {@link ScanConsumer#onNext} is invoked for each child, then either
     * {@link ScanConsumer#onCompleted} (success) or {@link ScanConsumer#onError} (failure)
     * exactly once. The returned future completes when the scan terminates and mirrors the
     * terminal callback — callers may rely on either.
     *
     * <p>Backends with a native range-scan primitive (Oxia, RocksDB, in-memory NavigableMap)
     * issue a single store-side scan. Other backends fall back to {@link #getChildren} +
     * sequential {@link #get}, at the cost of one extra round trip per child.
     *
     * @param parentPath path whose direct children should be streamed
     * @param consumer   callback that receives records, completion, or an error
     * @param opts       the set of {@link Option options} for this operation
     * @return a future that completes when the scan terminates
     */
    default CompletableFuture<Void> scanChildren(String parentPath, ScanConsumer consumer, Set<Option> opts) {
        return CompletableFuture.failedFuture(
                new MetadataStoreException("scanChildren not supported by this store"));
    }

    /**
     * Like {@link #scanChildren(String, ScanConsumer, Set)} with no options.
     */
    default CompletableFuture<Void> scanChildren(String parentPath, ScanConsumer consumer) {
        return scanChildren(parentPath, consumer, Set.of());
    }

    /**
     * Subscribe to updates on a sequence-key prefix written via {@link Option.SequenceKeysDeltas}.
     *
     * <p>The {@code listener} receives the latest assigned sequence key (the full path with
     * sequence suffix) as new sequence records are created under {@code prefix}. Multiple updates
     * may be collapsed into a single event with the highest sequence — callers must treat the
     * stream as monotonic but not exhaustive.
     *
     * <p>Closing the returned handle unsubscribes. On Oxia this delegates to the native
     * sequence-update channel; other backends synthesize the stream from change notifications on
     * the prefix's parent path.
     *
     * @param prefix   the sequence-key prefix (the same string passed as {@code path} to a
     *                 {@code put} with {@link Option.SequenceKeysDeltas})
     * @param listener callback receiving the full path of the latest sequence key
     * @param opts     the set of {@link Option options} for this subscription
     * @return a handle whose {@link AutoCloseable#close} cancels the subscription
     * @throws MetadataStoreException if the store doesn't support sequence subscriptions
     */
    default AutoCloseable subscribeSequence(String prefix, Consumer<String> listener, Set<Option> opts)
            throws MetadataStoreException {
        throw new MetadataStoreException("Sequence subscriptions not supported by this store");
    }

    /** Like {@link #subscribeSequence(String, Consumer, Set)} with no options. */
    default AutoCloseable subscribeSequence(String prefix, Consumer<String> listener)
            throws MetadataStoreException {
        return subscribeSequence(prefix, listener, Set.of());
    }

    /**
     * Returns the default metadata cache config.
     *
     * @return default metadata cache config
     */
    default MetadataCacheConfig<?> getDefaultMetadataCacheConfig() {
        return MetadataCacheConfig.builder().build();
    }
}
