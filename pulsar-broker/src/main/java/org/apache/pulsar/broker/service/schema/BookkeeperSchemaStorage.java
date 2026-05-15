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
package org.apache.pulsar.broker.service.schema;

import static java.util.Objects.isNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorage.Functions.newSchemaEntry;
import static org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import static org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import lombok.CustomLog;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.impl.LedgerMetadataUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.exceptions.SchemaException;
import org.apache.pulsar.common.policies.data.SchemaMetadata;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.protocol.schema.StoredSchema;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataSerde;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.jspecify.annotations.NonNull;

@CustomLog
public class BookkeeperSchemaStorage implements SchemaStorage {
    private static final String SchemaPath = "/schemas";
    private static final byte[] LedgerPassword = "".getBytes();

    private final MetadataStoreExtended store;
    private final PulsarService pulsar;
    private final MetadataCache<SchemaLocator> locatorEntryCache;

    private final ServiceConfiguration config;
    private BookKeeper bookKeeper;

    private final ConcurrentMap<String, CompletableFuture<StoredSchema>> readSchemaOperations =
            new ConcurrentHashMap<>();

    @VisibleForTesting
    BookkeeperSchemaStorage(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.store = pulsar.getLocalMetadataStore();
        this.config = pulsar.getConfiguration();
        this.locatorEntryCache = store.getMetadataCache(new MetadataSerde<SchemaLocator>() {
            @Override
            public byte[] serialize(String path, SchemaLocator value) {
                return value.toByteArray();
            }

            @Override
            public SchemaLocator deserialize(String path, byte[] content, Stat stat)
                    throws IOException {
                SchemaLocator loc = new SchemaLocator();
                loc.parseFrom(content);
                return loc;
            }
        });
    }

    @Override
    public void start() throws IOException {
        this.bookKeeper = pulsar.getBookKeeperClientFactory().create(
            pulsar.getConfiguration(),
            store,
            pulsar.getIoEventLoopGroup(),
            Optional.empty(),
            null
        ).join();
    }

    @Override
    public CompletableFuture<SchemaVersion> put(String key, byte[] value, byte[] hash) {
        return putSchema(key, value, hash).thenApply(LongSchemaVersion::new);
    }

    @Override
    public CompletableFuture<SchemaVersion> put(String key,
            Function<CompletableFuture<List<CompletableFuture<StoredSchema>>>,
                    CompletableFuture<Pair<byte[], byte[]>>> fn) {
        CompletableFuture<SchemaVersion> promise = new CompletableFuture<>();
        put(key, fn, promise);
        return promise;
    }

    private void put(String key,
             Function<CompletableFuture<List<CompletableFuture<StoredSchema>>>,
             CompletableFuture<Pair<byte[], byte[]>>> fn,
             CompletableFuture<SchemaVersion> promise) {
        CompletableFuture<Pair<Optional<LocatorEntry>, List<CompletableFuture<StoredSchema>>>> schemasWithLocator =
                getAllWithLocator(key);
        schemasWithLocator.thenCompose(pair ->
                fn.apply(completedFuture(pair.getRight())).thenCompose(p -> {
                    // The schema is existed
                    if (p == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return putSchema(key, p.getLeft(), p.getRight(), pair.getLeft());
                }).thenApply(version -> {
                    return version != null ? new LongSchemaVersion(version) : null;
                })).whenComplete((v, ex) -> {
                    if (ex == null) {
                        promise.complete(v);
                    } else {
                        Throwable cause = FutureUtil.unwrapCompletionException(ex);
                        if (cause instanceof AlreadyExistsException || cause instanceof BadVersionException) {
                            put(key, fn, promise);
                        } else {
                            promise.completeExceptionally(ex);
                        }
                    }
        });
    }

    @Override
    public CompletableFuture<StoredSchema> get(String key, SchemaVersion version) {
        if (version == SchemaVersion.Latest) {
            return getSchema(key);
        } else {
            LongSchemaVersion longVersion = (LongSchemaVersion) version;
            return getSchema(key, longVersion.getVersion());
        }
    }

    @Override
    public CompletableFuture<List<CompletableFuture<StoredSchema>>> getAll(String key) {
        return getAllWithLocator(key).thenApply(Pair::getRight);
    }

    private CompletableFuture<Pair<Optional<LocatorEntry>, List<CompletableFuture<StoredSchema>>>> getAllWithLocator(
            String key) {
        return getLocator(key).thenApply(locator -> {
            log.debug().attr("key", key).attr("locator", locator).log("Get all schemas");

            if (locator.isEmpty()) {
                return Pair.of(locator, Collections.emptyList());
            }

            SchemaLocator schemaLocator = locator.get().locator;
            List<CompletableFuture<StoredSchema>> list = new ArrayList<>();
            for (int i = 0; i < schemaLocator.getIndexsCount(); i++) {
                IndexEntry indexEntry = schemaLocator.getIndexAt(i);
                list.add(readSchemaEntry(indexEntry.getPosition())
                        .thenApply(entry -> new StoredSchema(
                                entry.getSchemaData(),
                                new LongSchemaVersion(indexEntry.getVersion())
                        ))
                );
            }
            return Pair.of(locator, list);
        });
    }

    CompletableFuture<Optional<LocatorEntry>> getLocator(String key) {
        return getSchemaLocator(getSchemaPath(key));
    }

    public List<Long> getSchemaLedgerList(String key) throws IOException {
        Optional<LocatorEntry> locatorEntry = null;
        try {
            locatorEntry = getLocator(key).get();
        } catch (Exception e) {
            log.warn()
                    .attr("key", key)
                    .exceptionMessage(e)
                    .log("Failed to get list of schema-storage ledger");
            throw new IOException("Failed to get schema ledger for" + key);
        }
        LocatorEntry entry = locatorEntry.orElse(null);
        if (entry == null) {
            return null;
        }
        List<Long> ledgerIds = new ArrayList<>(entry.locator.getIndexsCount());
        for (int i = 0; i < entry.locator.getIndexsCount(); i++) {
            ledgerIds.add(entry.locator.getIndexAt(i).getPosition().getLedgerId());
        }
        return ledgerIds;
    }

    @VisibleForTesting
    BookKeeper getBookKeeper() {
        return bookKeeper;
    }

    @Override
    public CompletableFuture<SchemaVersion> delete(String key, boolean forcefully) {
        return deleteSchema(key, forcefully).thenApply(version -> {
            if (version == null) {
                return null;
            }
            return new LongSchemaVersion(version);
        });
    }

    @Override
    public CompletableFuture<SchemaVersion> delete(String key) {
        return delete(key, false);
    }

    @NonNull
    private CompletableFuture<StoredSchema> getSchema(String schemaId) {
        // There's already a schema read operation in progress. Just piggyback on that
        return readSchemaOperations.computeIfAbsent(schemaId, key -> {
            log.debug().attr("schemaId", schemaId).log("Fetching schema from store");
            return getSchemaLocator(getSchemaPath(schemaId)).thenCompose(locator -> {
                log.debug().attr("schemaId", schemaId).attr("locator", locator).log("Got schema locator");
                if (!locator.isPresent()) {
                    return completedFuture(null);
                }

                SchemaLocator schemaLocator = locator.get().locator;

                return readSchemaEntry(schemaLocator.getInfo().getPosition())
                        .thenApply(entry -> new StoredSchema(entry.getSchemaData(),
                                new LongSchemaVersion(schemaLocator.getInfo().getVersion())));
            });
        }).whenComplete((res, ex) -> {
            log.debug()
                    .attr("schemaId", schemaId)
                    .attr("result", res)
                    .exceptionMessage(ex)
                    .log("Get operation completed");
            readSchemaOperations.remove(schemaId);
        });
    }

    @Override
    public SchemaVersion versionFromBytes(byte[] version) {
        // The schema storage converts the schema from bytes to long
        // so it handles both cases 1) version is 64 bytes long pre 2.4.0;
        // 2) version is 8 bytes long post 2.4.0
        //
        // NOTE: if you are planning to change the logic here. you should consider
        //       both 64 bytes and 8 bytes cases.
        ByteBuffer bb = ByteBuffer.wrap(version);
        return new LongSchemaVersion(bb.getLong());
    }

    @Override
    public void close() throws Exception {
        if (bookKeeper != null) {
            bookKeeper.close();
        }
    }

    @NonNull
    private CompletableFuture<StoredSchema> getSchema(String schemaId, long version) {
        log.debug().attr("schemaId", schemaId).attr("version", version).log("Get schema");

        return getSchemaLocator(getSchemaPath(schemaId)).thenCompose(locator -> {
            log.debug()
                    .attr("schemaId", schemaId)
                    .attr("version", version)
                    .attr("locator", locator)
                    .log("Get schema locator");

            if (!locator.isPresent()) {
                return completedFuture(null);
            }

            SchemaLocator schemaLocator = locator.get().locator;
            if (version > schemaLocator.getInfo().getVersion()) {
                return completedFuture(null);
            }

            List<IndexEntry> indexList = new ArrayList<>(schemaLocator.getIndexsCount());
            for (int i = 0; i < schemaLocator.getIndexsCount(); i++) {
                indexList.add(schemaLocator.getIndexAt(i));
            }
            return findSchemaEntryByVersion(indexList, version)
                .thenApply(entry ->
                        new StoredSchema(
                            entry.getSchemaData(),
                            new LongSchemaVersion(version)
                        )
                );
        });
    }

    @NonNull
    private CompletableFuture<Long> putSchema(String schemaId, byte[] data, byte[] hash) {
        return getSchemaLocator(getSchemaPath(schemaId)).thenCompose(optLocatorEntry ->
                putSchema(schemaId, data, hash, optLocatorEntry));
    }

    private CompletableFuture<Long> putSchema(String schemaId, byte[] data, byte[] hash,
                                              Optional<LocatorEntry> optLocatorEntry) {
        if (optLocatorEntry.isPresent()) {

            SchemaLocator locator = optLocatorEntry.get().locator;

            log.debug().attr("schemaId", schemaId).attr("hash", hash).log("findSchemaEntryByHash");

            //don't check the schema whether already exist
            List<IndexEntry> indexList = new ArrayList<>(locator.getIndexsCount());
            for (int i = 0; i < locator.getIndexsCount(); i++) {
                indexList.add(locator.getIndexAt(i));
            }
            return readSchemaEntry(indexList.get(0).getPosition())
                    .thenCompose(schemaEntry -> addNewSchemaEntryToStore(schemaId,
                            indexList, data).thenCompose(
                            position -> updateSchemaLocator(schemaId, optLocatorEntry.get(), position, hash))
                    );
        } else {
            return createNewSchema(schemaId, data, hash);
        }
    }

    private CompletableFuture<Long> createNewSchema(String schemaId, byte[] data, byte[] hash) {
        // Step 1: Store the schema data into a new BookKeeper ledger
        IndexEntry emptyIndex = new IndexEntry();
        emptyIndex.setVersion(0);
        emptyIndex.setHash(hash);
        emptyIndex.setPosition().setEntryId(-1L).setLedgerId(-1L);
        CompletableFuture<PositionInfo> stored = addNewSchemaEntryToStore(
                schemaId, Collections.singletonList(emptyIndex), data
        );

        return stored.thenCompose(position -> {
            // Step 2: Create the schema locator z-node pointing to the ledger
            IndexEntry info = new IndexEntry();
            info.setVersion(0);
            info.setPosition().copyFrom(position);
            info.setHash(hash);
            SchemaLocator locator = new SchemaLocator();
            locator.setInfo().copyFrom(info);
            locator.addIndex().copyFrom(info);
            // Step 3: Handle failure by cleaning up the orphan ledger
            // if concurrent schema creation caused a CAS conflict
            return createSchemaLocator(getSchemaPath(schemaId), locator)
                    .thenApply(ignore -> 0L)
                    .exceptionallyCompose(ex -> {
                        Throwable cause = FutureUtil.unwrapCompletionException(ex);
                        log.warn()
                                .attr("schemaId", schemaId)
                                .attr("ledgerId", position.getLedgerId())
                                .exception(cause)
                                .log("Failed to create schema locator with position");
                        if (cause instanceof AlreadyExistsException || cause instanceof BadVersionException) {
                            return deleteLedgerAsync(schemaId, position.getLedgerId(),
                                    "schema locator creation failed")
                                    .thenCompose(__ -> FutureUtil.failedFuture(cause));
                        }
                        return FutureUtil.failedFuture(cause);
                    });
        });
    }

    @NonNull
    private CompletableFuture<Long> deleteSchema(String schemaId, boolean forcefully) {
        return (forcefully ? CompletableFuture.completedFuture(null)
                : ignoreUnrecoverableBKException(getSchema(schemaId))).thenCompose(schemaAndVersion -> {
            if (!forcefully && isNull(schemaAndVersion)) {
                return completedFuture(null);
            } else {
                // The version is only for the compatibility of the current interface
                final long version = -1;
                CompletableFuture<Long> future = new CompletableFuture<>();
                getLocator(schemaId).whenComplete((locator, ex) -> {
                    if (ex != null) {
                        future.completeExceptionally(ex);
                    } else {
                        if (!locator.isPresent()) {
                            future.complete(null);
                            return;
                        }
                        SchemaLocator schemaLocator = locator.get().locator;
                        List<CompletableFuture<Void>> deleteFutures = new ArrayList<>(schemaLocator.getIndexsCount());
                        for (int i = 0; i < schemaLocator.getIndexsCount(); i++) {
                            IndexEntry indexEntry = schemaLocator.getIndexAt(i);
                            final long ledgerId = indexEntry.getPosition().getLedgerId();
                            CompletableFuture<Void> deleteFuture = new CompletableFuture<>();
                            deleteFutures.add(deleteFuture);
                            bookKeeper.asyncDeleteLedger(ledgerId, (int rc, Object cnx) -> {
                                if (rc != BKException.Code.OK) {
                                    // It's not a serious error, we didn't need call future.completeExceptionally()
                                    log.warn()
                                            .attr("ledgerId", ledgerId)
                                            .attr("schemaId", schemaId)
                                            .attr("rc", rc)
                                            .log("Failed to delete ledger of");
                                }
                                deleteFuture.complete(null);
                            }, null);
                        }
                        FutureUtil.waitForAll(deleteFutures).whenComplete((v, e) -> {
                            final String path = getSchemaPath(schemaId);
                            store.delete(path, Optional.empty())
                                    .thenRun(() -> {
                                        future.complete(version);
                                    }).exceptionally(zkException -> {
                                        if (zkException.getCause()
                                                instanceof MetadataStoreException.NotFoundException) {
                                            // The znode has been deleted by others.
                                            // In some cases, the program may enter this logic.
                                            // Since the znode is gone, we don’t need to deal with it.
                                            log.debug().attr("path", path).log("No node for schema path");
                                            future.complete(null);
                                        } else {
                                            future.completeExceptionally(zkException);
                                        }
                                        return null;
                            });
                        });
                    }
                });
                return future;
            }
        });
    }

    @NonNull
    private static String getSchemaPath(String schemaId) {
        return SchemaPath + "/" + schemaId;
    }

    @NonNull
    private CompletableFuture<PositionInfo> addNewSchemaEntryToStore(
        String schemaId,
        List<IndexEntry> index,
        byte[] data
    ) {
        SchemaEntry schemaEntry = newSchemaEntry(index, data);
        return createLedger(schemaId).thenCompose(ledgerHandle -> {
            final long ledgerId = ledgerHandle.getId();
            return addEntry(ledgerHandle, schemaEntry)
                    .thenApply(entryId -> {
                        ledgerHandle.closeAsync();
                        return Functions.newPositionInfo(ledgerId, entryId);
                    });
        });
    }

    @NonNull
    private CompletableFuture<Long> updateSchemaLocator(
        String schemaId,
        LocatorEntry locatorEntry,
        PositionInfo position,
        byte[] hash
    ) {
        long nextVersion = locatorEntry.locator.getInfo().getVersion() + 1;
        SchemaLocator locator = locatorEntry.locator;
        IndexEntry info = new IndexEntry();
        info.setVersion(nextVersion);
        info.setPosition().copyFrom(position);
        info.setHash(hash);

        SchemaLocator newLocator = new SchemaLocator();
        newLocator.setInfo().copyFrom(info);
        for (int i = 0; i < locator.getIndexsCount(); i++) {
            newLocator.addIndex().copyFrom(locator.getIndexAt(i));
        }
        newLocator.addIndex().copyFrom(info);
        return updateSchemaLocator(getSchemaPath(schemaId),
                newLocator
                , locatorEntry.version
        ).thenApply(ignore -> nextVersion).exceptionallyCompose(ex -> {
            Throwable cause = FutureUtil.unwrapCompletionException(ex);
            log.warn()
                    .attr("schemaId", schemaId)
                    .attr("ledgerId", position.getLedgerId())
                    .exception(cause)
                    .log("Failed to update schema locator with position");
            if (cause instanceof AlreadyExistsException || cause instanceof BadVersionException) {
                return deleteLedgerAsync(schemaId, position.getLedgerId(),
                        "schema locator update failed")
                        .thenCompose(__ -> FutureUtil.failedFuture(cause));
            }
            return FutureUtil.failedFuture(cause);
        });
    }

    private CompletableFuture<Void> deleteLedgerAsync(String schemaId, long ledgerId, String reason) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            bookKeeper.asyncDeleteLedger(ledgerId, (rc, ctx) -> {
                if (rc != BKException.Code.OK) {
                    log.warn()
                            .attr("schemaId", schemaId)
                            .attr("ledgerId", ledgerId)
                            .attr("rc", rc)
                            .attr("reason", reason)
                            .log("Failed to delete orphan schema ledger");
                }
                future.complete(null);
            }, null);
        } catch (Throwable t) {
            log.warn()
                    .attr("schemaId", schemaId)
                    .attr("ledgerId", ledgerId)
                    .attr("reason", reason)
                    .exception(t)
                    .log("Failed to trigger orphan schema ledger deletion");
            future.complete(null);
        }
        return future;
    }

    @NonNull
    private CompletableFuture<SchemaEntry> findSchemaEntryByVersion(
        List<IndexEntry> index,
        long version
    ) {

        if (index.isEmpty()) {
            return completedFuture(null);
        }

        IndexEntry lowest = index.get(0);
        if (version < lowest.getVersion()) {
            return readSchemaEntry(lowest.getPosition())
                    .thenCompose(entry -> {
                        List<IndexEntry> entryIndex = new ArrayList<>(entry.getIndexsCount());
                        for (int i = 0; i < entry.getIndexsCount(); i++) {
                            entryIndex.add(entry.getIndexAt(i));
                        }
                        return findSchemaEntryByVersion(entryIndex, version);
                    });
        }

        for (IndexEntry entry : index) {
            if (entry.getVersion() == version) {
                return readSchemaEntry(entry.getPosition());
            } else if (entry.getVersion() > version) {
                break;
            }
        }

        return completedFuture(null);
    }

    @NonNull
    private CompletableFuture<SchemaEntry> readSchemaEntry(
        PositionInfo position
    ) {
        log.debug().attr("position", position).log("Reading schema entry from");

        return openLedger(position.getLedgerId())
            .thenCompose((ledger) ->
                Functions.getLedgerEntry(ledger, position.getEntryId(), config.isSchemaLedgerForceRecovery())
                    .thenCompose(entry -> closeLedger(ledger)
                        .thenApply(ignore -> entry)
                    )
            ).thenCompose(Functions::parseSchemaEntry);
    }

    @NonNull
    private CompletableFuture<Void> updateSchemaLocator(String id,
                                                        SchemaLocator schema, long version) {
        return store.put(id, schema.toByteArray(), Optional.of(version)).thenApply(__ -> null);
    }

    @NonNull
    private CompletableFuture<LocatorEntry> createSchemaLocator(String id, SchemaLocator locator) {
        return store.put(id, locator.toByteArray(), Optional.of(-1L))
                .thenApply(stat -> new LocatorEntry(locator, stat.getVersion()));
    }

    @NonNull
    private CompletableFuture<Optional<LocatorEntry>> getSchemaLocator(String schema) {
        return locatorEntryCache.getWithStats(schema)
                .thenApply(o ->
                        o.map(r -> new LocatorEntry(r.getValue(), r.getStat().getVersion())));
    }

    public CompletableFuture<SchemaMetadata> getSchemaMetadata(String schema) {
        return getLocator(schema).thenApply(locator -> {
            if (!locator.isPresent()) {
                return null;
            }
            SchemaLocator sl = locator.get().locator;
            SchemaMetadata metadata = new SchemaMetadata();
            IndexEntry info = sl.getInfo();
            metadata.info = new SchemaMetadata.Entry(info.getPosition().getLedgerId(), info.getPosition().getEntryId(),
                    info.getVersion());
            List<SchemaMetadata.Entry> indexEntries = new ArrayList<>(sl.getIndexsCount());
            for (int i = 0; i < sl.getIndexsCount(); i++) {
                IndexEntry idx = sl.getIndexAt(i);
                indexEntries.add(new SchemaMetadata.Entry(idx.getPosition().getLedgerId(),
                        idx.getPosition().getEntryId(), idx.getVersion()));
            }
            metadata.index = indexEntries;
            return metadata;
        });
    }

    @NonNull
    private CompletableFuture<Long> addEntry(LedgerHandle ledgerHandle, SchemaEntry entry) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        ledgerHandle.asyncAddEntry(entry.toByteArray(),
            (rc, handle, entryId, ctx) -> {
                if (rc != BKException.Code.OK) {
                    future.completeExceptionally(bkException("Failed to add entry", rc, ledgerHandle.getId(), -1,
                            config.isSchemaLedgerForceRecovery()));
                } else {
                    future.complete(entryId);
                }
            }, null
        );
        return future;
    }

    @NonNull
    private CompletableFuture<LedgerHandle> createLedger(String schemaId) {
        Map<String, byte[]> metadata = LedgerMetadataUtils.buildMetadataForSchema(schemaId);
        final CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        try {
            bookKeeper.asyncCreateLedger(
                    config.getManagedLedgerDefaultEnsembleSize(),
                    config.getManagedLedgerDefaultWriteQuorum(),
                    config.getManagedLedgerDefaultAckQuorum(),
                    BookKeeper.DigestType.fromApiDigestType(config.getManagedLedgerDigestType()),
                    LedgerPassword,
                    (rc, handle, ctx) -> {
                        if (rc != BKException.Code.OK) {
                            future.completeExceptionally(bkException("Failed to create ledger", rc, -1, -1,
                                    config.isSchemaLedgerForceRecovery()));
                        } else {
                            future.complete(handle);
                        }
                    }, null, metadata);
        } catch (Throwable t) {
            log.error()
                    .attr("schemaId", schemaId)
                    .exception(t)
                    .log("Encountered unexpected error when creating schema ledger");
            return FutureUtil.failedFuture(t);
        }
        return future;
    }

    @NonNull
    private CompletableFuture<LedgerHandle> openLedger(Long ledgerId) {
        final CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        bookKeeper.asyncOpenLedger(
            ledgerId,
            BookKeeper.DigestType.fromApiDigestType(config.getManagedLedgerDigestType()),
            LedgerPassword,
            (rc, handle, ctx) -> {
                if (rc != BKException.Code.OK) {
                    future.completeExceptionally(bkException("Failed to open ledger", rc, ledgerId, -1,
                            config.isSchemaLedgerForceRecovery()));
                } else {
                    future.complete(handle);
                }
            }, null, true
        );
        return future;
    }

    @NonNull
    private CompletableFuture<Void> closeLedger(LedgerHandle ledgerHandle) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        ledgerHandle.asyncClose((rc, handle, ctx) -> {
            if (rc != BKException.Code.OK) {
                future.completeExceptionally(bkException("Failed to close ledger", rc, ledgerHandle.getId(), -1,
                        config.isSchemaLedgerForceRecovery()));
            } else {
                future.complete(null);
            }
        }, null);
        return future;
    }

    public CompletableFuture<List<Long>> getStoreLedgerIdsBySchemaId(String schemaId) {
        CompletableFuture<List<Long>> ledgerIdsFuture = new CompletableFuture<>();
        getSchemaLocator(getSchemaPath(schemaId)).thenAccept(locator -> {
            log.debug()
                    .attr("schemaId", schemaId)
                    .attr("locator", locator)
                    .log("Get all store schema ledgerIds - locator");

            if (!locator.isPresent()) {
                ledgerIdsFuture.complete(Collections.emptyList());
                return;
            }
            Set<Long> ledgerIds = new HashSet<>();
            SchemaLocator schemaLocator = locator.get().locator;
            for (int i = 0; i < schemaLocator.getIndexsCount(); i++) {
                ledgerIds.add(schemaLocator.getIndexAt(i).getPosition().getLedgerId());
            }
            ledgerIdsFuture.complete(new ArrayList<>(ledgerIds));
        }).exceptionally(e -> {
            ledgerIdsFuture.completeExceptionally(e);
            return null;
        });
        return ledgerIdsFuture;
    }

    interface Functions {
        static CompletableFuture<LedgerEntry> getLedgerEntry(LedgerHandle ledger, long entry,
                boolean forceRecovery) {
            final CompletableFuture<LedgerEntry> future = new CompletableFuture<>();
            ledger.asyncReadEntries(entry, entry,
                (rc, handle, entries, ctx) -> {
                    if (rc != BKException.Code.OK) {
                        future.completeExceptionally(bkException("Failed to read entry", rc, ledger.getId(), entry,
                                forceRecovery));
                    } else {
                        future.complete(entries.nextElement());
                    }
                }, null
            );
            return future;
        }

        static CompletableFuture<SchemaEntry> parseSchemaEntry(LedgerEntry ledgerEntry) {
            CompletableFuture<SchemaEntry> result = new CompletableFuture<>();
            try {
                byte[] data = ledgerEntry.getEntry();
                SchemaEntry entry = new SchemaEntry();
                entry.parseFrom(data);
                result.complete(entry);
            } catch (Exception e) {
                result.completeExceptionally(e);
            }
            return result;
        }

        static SchemaEntry newSchemaEntry(
            List<IndexEntry> index,
            byte[] data
        ) {
            SchemaEntry entry = new SchemaEntry();
            entry.setSchemaData(data);
            for (int i = 0; i < index.size(); i++) {
                entry.addIndex().copyFrom(index.get(i));
            }
            return entry;
        }

        static PositionInfo newPositionInfo(long ledgerId, long entryId) {
            PositionInfo pos = new PositionInfo();
            pos.setLedgerId(ledgerId);
            pos.setEntryId(entryId);
            return pos;
        }
    }

    static class LocatorEntry {
        final SchemaLocator locator;
        final long version;

        LocatorEntry(SchemaLocator locator, long version) {
            this.locator = locator;
            this.version = version;
        }
    }

    public static Exception bkException(String operation, int rc, long ledgerId, long entryId,
            boolean forceRecovery) {
        String message = org.apache.bookkeeper.client.api.BKException.getMessage(rc)
                + " -  ledger=" + ledgerId + " - operation=" + operation;

        if (entryId != -1) {
            message += " - entry=" + entryId;
        }
        boolean recoverable = rc != BKException.Code.NoSuchLedgerExistsException
                && rc != BKException.Code.NoSuchEntryException
                && rc != BKException.Code.NoSuchLedgerExistsOnMetadataServerException
                // if force-recovery is enabled then made it non-recoverable exception
                // and force schema to skip this exception and recover immediately
                && !forceRecovery;
        return new SchemaException(recoverable, message);
    }

    public static <T> CompletableFuture<T> ignoreUnrecoverableBKException(CompletableFuture<T> source) {
        return source.exceptionally(t -> {
            if (t.getCause() != null
                    && (t.getCause() instanceof SchemaException)
                    && !(t.getCause() instanceof IncompatibleSchemaException)
                    && !((SchemaException) t.getCause()).isRecoverable()) {
                // Meeting NoSuchLedgerExistsException, NoSuchEntryException or
                // NoSuchLedgerExistsOnMetadataServerException when reading schemas in
                // bookkeeper. This also means that the data has already been deleted by other operations
                // in deleting schema.
                log.debug().exception(t).log("Schema data in bookkeeper may be deleted by other operations.");
                return null;
            }
            // rethrow other cases
            throw t instanceof CompletionException ? (CompletionException) t : new CompletionException(t);
        });
    }
}
