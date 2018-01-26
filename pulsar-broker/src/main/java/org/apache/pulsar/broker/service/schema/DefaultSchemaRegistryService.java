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
package org.apache.pulsar.broker.service.schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.schema.SchemaRegistryFormat;
import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.protobuf.ByteString.copyFrom;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.pulsar.broker.service.schema.DefaultSchemaRegistryService.Functions.newSchemaEntry;
import static org.apache.pulsar.client.util.FutureUtil.completedFuture;

public class DefaultSchemaRegistryService implements SchemaRegistryService {
    private static final String SchemaPath = "/schemas";
    private static final HashFunction HashFunction = Hashing.sha1();
    private static final List<ACL> Acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    private final PulsarService pulsar;
    private final ZooKeeper zooKeeper;
    private final ZooKeeperCache localZkCache;
    private final Clock clock;
    private BookKeeper bookKeeper;

    public static DefaultSchemaRegistryService create(PulsarService pulsar) throws KeeperException, InterruptedException {
        DefaultSchemaRegistryService service = new DefaultSchemaRegistryService(pulsar);
        service.init();
        return service;
    }

    @VisibleForTesting
    public DefaultSchemaRegistryService(PulsarService pulsar, Clock clock) {
        this.pulsar = pulsar;
        this.localZkCache = pulsar.getLocalZkCache();
        this.zooKeeper = localZkCache.getZooKeeper();
        this.clock = clock;
    }

    private DefaultSchemaRegistryService(PulsarService pulsar) {
        this(pulsar, Clock.systemUTC());
    }

    @VisibleForTesting
    public void init() throws KeeperException, InterruptedException {
        try {
            zooKeeper.create(SchemaPath, new byte[]{}, Acl, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException error) {
            // race on startup, ignore.
        }
    }

    @Override
    public void start() throws IOException {
        this.bookKeeper = pulsar.getBookKeeperClientFactory().create(
            pulsar.getConfiguration(),
            pulsar.getZkClient()
        );
    }

    @Override
    @NotNull
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId) {
        return getSchemaLocator(getSchemaPath(schemaId)).thenCompose(locator -> {

            if (!locator.isPresent()) {
                return completedFuture(null);
            }

            SchemaRegistryFormat.SchemaLocator schemaLocator = locator.get().locator;
            long version = schemaLocator.getInfo().getVersion();
            return readSchemaEntry(schemaLocator.getPosition())
                .thenApply(Functions::schemaEntryToSchema)
                .thenApply(schema -> new SchemaAndMetadata(schemaId, schema, version));
        });
    }

    @Override
    @NotNull
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, long version) {
        return getSchemaLocator(getSchemaPath(schemaId)).thenCompose(locator -> {

            if (!locator.isPresent()) {
                return completedFuture(null);
            }

            SchemaRegistryFormat.SchemaLocator schemaLocator = locator.get().locator;
            if (version > schemaLocator.getInfo().getVersion()) {
                return completedFuture(null);
            }

            return findSchemaEntry(schemaLocator.getIndexList(), version)
                .thenApply(Functions::schemaEntryToSchema)
                .thenApply(schema -> new SchemaAndMetadata(schemaId, schema, version));
        });
    }

    @Override
    @NotNull
    public CompletableFuture<Long> putSchema(String schemaId, Schema schema) {
        return getOrCreateSchemaLocator(getSchemaPath(schemaId)).thenCompose(locatorEntry -> {
            long nextVersion = locatorEntry.locator.getInfo().getVersion() + 1;
            return addNewSchemaEntryToStore(locatorEntry.locator.getIndexList(), schemaId, schema, nextVersion).thenCompose(position ->
                updateSchemaLocator(locatorEntry, position, schemaId, schema, nextVersion)
            );
        });
    }

    @Override
    @NotNull
    public CompletableFuture<Void> deleteSchema(String schemaId, String user) {
        return getSchema(schemaId).thenCompose(schemaAndVersion -> {
            if (isNull(schemaAndVersion)) {
                return completedFuture(null);
            } else {
                Schema schema = schemaAndVersion.schema;
                return putSchema(
                    schemaId,
                    Schema.newBuilder()
                        .isDeleted(true)
                        .data(new byte[]{})
                        .timestamp(clock.millis())
                        .type(SchemaType.NONE)
                        .user(user)
                        .build()
                );
            }
        }).thenApply(ignore -> null);
    }

    @Override
    public void close() throws Exception {
        if (nonNull(bookKeeper)) {
            bookKeeper.close();
        }
    }

    private String getSchemaPath(String schemaId) {
        return SchemaPath + "/" + schemaId;
    }

    private CompletableFuture<SchemaRegistryFormat.PositionInfo> addNewSchemaEntryToStore(
        List<SchemaRegistryFormat.IndexEntry> index,
        String schemaId,
        Schema schema,
        long version
    ) {
        SchemaRegistryFormat.SchemaEntry schemaEntry = newSchemaEntry(index, schemaId, schema, version);
        return createLedger().thenCompose(ledgerHandle ->
            addEntry(ledgerHandle, schemaEntry).thenApply(entryId ->
                Functions.newPositionInfo(ledgerHandle.getId(), entryId)
            )
        );
    }

    private CompletableFuture<Long> updateSchemaLocator(
        LocatorEntry locatorEntry,
        SchemaRegistryFormat.PositionInfo position,
        String schemaId,
        Schema schema,
        long nextVersion
    ) {
        SchemaRegistryFormat.SchemaLocator locator = locatorEntry.locator;
        return updateSchemaLocator(getSchemaPath(schemaId),
            SchemaRegistryFormat.SchemaLocator.newBuilder()
                .setInfo(Functions.buildSchemaInfo(schemaId, schema, nextVersion))
                .setPosition(position)
                .addAllIndex(Functions.buildIndex(
                    locator.getIndexList(),
                    position,
                    nextVersion)
                ).build(), locatorEntry.version
        ).thenApply(ignore -> nextVersion);
    }

    private CompletableFuture<SchemaRegistryFormat.SchemaEntry> findSchemaEntry(
        List<SchemaRegistryFormat.IndexEntry> index,
        long version
    ) {

        if (index.isEmpty()) {
            return completedFuture(null);
        }

        SchemaRegistryFormat.IndexEntry lowest = index.get(0);
        if (version < lowest.getVersion()) {
            return readSchemaEntry(lowest.getPosition())
                .thenCompose(entry -> findSchemaEntry(entry.getIndexList(), version));
        }

        for (SchemaRegistryFormat.IndexEntry entry : index) {
            if (entry.getVersion() == version) {
                return readSchemaEntry(entry.getPosition());
            } else if (entry.getVersion() > version) {
                break;
            }
        }

        return completedFuture(null);
    }

    private CompletableFuture<SchemaRegistryFormat.SchemaEntry> readSchemaEntry(
        SchemaRegistryFormat.PositionInfo position
    ) {
        return openLedger(position.getLedgerId())
            .thenCompose((ledger) ->
                Functions.getLedgerEntry(ledger, position.getEntryId())
                    .thenCompose(entry -> closeLedger(ledger)
                        .thenApply(ignore -> entry)
                    )
            ).thenCompose(Functions::parseSchemaEntry);
    }

    private CompletableFuture<Void> updateSchemaLocator(String id, SchemaRegistryFormat.SchemaLocator schema, int version) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        zooKeeper.setData(id, schema.toByteArray(), version, (rc, path, ctx, stat) -> {
            Code code = Code.get(rc);
            if (code != Code.OK) {
                future.completeExceptionally(KeeperException.create(code));
            } else {
                future.complete(null);
            }
        }, null);
        return future;
    }

    private CompletableFuture<Optional<LocatorEntry>> getSchemaLocator(String schema) {
        return localZkCache.getEntryAsync(schema, new SchemaLocatorDeserializer()).thenApply(optional ->
            optional.map(entry -> new LocatorEntry(entry.getKey(), entry.getValue().getVersion()))
        );
    }

    private CompletableFuture<LocatorEntry> getOrCreateSchemaLocator(String schema) {
        return getSchemaLocator(schema).thenCompose(schemaLocatorStatEntry -> {
            if (schemaLocatorStatEntry.isPresent()) {
                return completedFuture(schemaLocatorStatEntry.get());
            } else {
                SchemaRegistryFormat.SchemaLocator locator = SchemaRegistryFormat.SchemaLocator.newBuilder()
                    .setPosition(SchemaRegistryFormat.PositionInfo.newBuilder()
                        .setEntryId(-1L)
                        .setLedgerId(-1L)
                    ).setInfo(SchemaRegistryFormat.SchemaInfo.newBuilder()
                        .setSchemaId(schema)
                        .setVersion(-1L)
                        .setType(SchemaRegistryFormat.SchemaInfo.SchemaType.NONE)
                    ).build();

                CompletableFuture<LocatorEntry> future = new CompletableFuture<>();

                zooKeeper.create(schema, locator.toByteArray(), Acl, CreateMode.PERSISTENT,
                    (rc, path, ctx, name) -> {
                        Code code = Code.get(rc);
                        if (code != Code.OK) {
                            future.completeExceptionally(KeeperException.create(code));
                        } else {
                            future.complete(new LocatorEntry(locator, -1));
                        }
                    }, null);
                return future;
            }
        });
    }

    private CompletableFuture<Long> addEntry(LedgerHandle ledgerHandle, SchemaRegistryFormat.SchemaEntry entry) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        ledgerHandle.asyncAddEntry(entry.toByteArray(),
            (rc, handle, entryId, ctx) -> {
                if (rc != BKException.Code.OK) {
                    future.completeExceptionally(BKException.create(rc));
                } else {
                    future.complete(entryId);
                }
            }, null
        );
        return future;
    }

    private CompletableFuture<LedgerHandle> createLedger() {
        final CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        bookKeeper.asyncCreateLedger(0, 0, DigestType.MAC, new byte[]{},
            (rc, handle, ctx) -> {
                if (rc != BKException.Code.OK) {
                    future.completeExceptionally(BKException.create(rc));
                } else {
                    future.complete(handle);
                }
            }, null
        );
        return future;
    }

    private CompletableFuture<LedgerHandle> openLedger(Long ledgerId) {
        final CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        bookKeeper.asyncOpenLedger(ledgerId, DigestType.MAC, new byte[]{},
            (rc, handle, ctx) -> {
                if (rc != BKException.Code.OK) {
                    future.completeExceptionally(BKException.create(rc));
                } else {
                    future.complete(handle);
                }
            }, null
        );
        return future;
    }

    private CompletableFuture<Void> closeLedger(LedgerHandle ledgerHandle) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        ledgerHandle.asyncClose((rc, handle, ctx) -> {
            if (rc != BKException.Code.OK) {
                future.completeExceptionally(BKException.create(rc));
            } else {
                future.complete(null);
            }
        }, null);
        return future;
    }

    static class SchemaLocatorDeserializer implements ZooKeeperCache.Deserializer<SchemaRegistryFormat.SchemaLocator> {
        @Override
        public SchemaRegistryFormat.SchemaLocator deserialize(String key, byte[] content) throws Exception {
            return SchemaRegistryFormat.SchemaLocator.parseFrom(content);
        }
    }

    interface Functions {
        static CompletableFuture<LedgerEntry> getLedgerEntry(LedgerHandle ledger, long entry) {
            final CompletableFuture<LedgerEntry> future = new CompletableFuture<>();
            ledger.asyncReadEntries(entry, entry,
                (rc, handle, entries, ctx) -> {
                    if (rc != BKException.Code.OK) {
                        future.completeExceptionally(BKException.create(rc));
                    } else {
                        future.complete(entries.nextElement());
                    }
                }, null
            );
            return future;
        }

        static CompletableFuture<SchemaRegistryFormat.SchemaEntry> parseSchemaEntry(LedgerEntry ledgerEntry) {
            CompletableFuture<SchemaRegistryFormat.SchemaEntry> result = new CompletableFuture<>();
            try {
                result.complete(SchemaRegistryFormat.SchemaEntry.parseFrom(ledgerEntry.getEntry()));
            } catch (IOException e) {
                result.completeExceptionally(e);
            }
            return result;
        }

        static SchemaRegistryFormat.SchemaInfo.SchemaType convertFromDomainType(SchemaType type) {
            switch (type) {
                case AVRO:
                    return SchemaRegistryFormat.SchemaInfo.SchemaType.AVRO;
                case JSON:
                    return SchemaRegistryFormat.SchemaInfo.SchemaType.JSON;
                case THRIFT:
                    return SchemaRegistryFormat.SchemaInfo.SchemaType.THRIFT;
                case PROTOBUF:
                    return SchemaRegistryFormat.SchemaInfo.SchemaType.PROTO;
                default:
                    return SchemaRegistryFormat.SchemaInfo.SchemaType.NONE;
            }
        }

        static SchemaType convertToDomainType(SchemaRegistryFormat.SchemaInfo.SchemaType type) {
            switch (type) {
                case AVRO:
                    return SchemaType.AVRO;
                case JSON:
                    return SchemaType.JSON;
                case PROTO:
                    return SchemaType.PROTOBUF;
                case THRIFT:
                    return SchemaType.THRIFT;
                default:
                    return SchemaType.NONE;
            }
        }

        static Schema schemaEntryToSchema(SchemaRegistryFormat.SchemaEntry entry) {
            return Schema.newBuilder()
                .isDeleted(entry.getIsDeleted())
                .data(entry.getSchemaData().toByteArray())
                .timestamp(entry.getInfo().getTimestamp())
                .type(convertToDomainType(entry.getInfo().getType()))
                .user(entry.getInfo().getAddedBy())
                .build();
        }

        static SchemaRegistryFormat.SchemaEntry newSchemaEntry(
            List<SchemaRegistryFormat.IndexEntry> index,
            String schemaId,
            Schema schema,
            long version
        ) {
            return SchemaRegistryFormat.SchemaEntry.newBuilder()
                .setInfo(SchemaRegistryFormat.SchemaInfo.newBuilder()
                    .setSchemaId(schemaId)
                    .setVersion(version)
                    .setAddedBy(schema.user)
                    .setType(Functions.convertFromDomainType(schema.type))
                    .setHash(copyFrom(
                        HashFunction.hashBytes(
                            schema.data
                        ).asBytes())
                    ).build())
                .setSchemaData(copyFrom(schema.data))
                .addAllIndex(index)
                .setIsDeleted(schema.isDeleted)
                .build();
        }

        static SchemaRegistryFormat.SchemaInfo buildSchemaInfo(String schemaId, Schema schema, long version) {
            return SchemaRegistryFormat.SchemaInfo.newBuilder()
                .setAddedBy(schema.user)
                .setVersion(version)
                .setSchemaId(schemaId)
                .setTimestamp(schema.timestamp)
                .setType(convertFromDomainType(schema.type))
                .build();
        }

        static SchemaRegistryFormat.PositionInfo newPositionInfo(long ledgerId, long entryId) {
            return SchemaRegistryFormat.PositionInfo.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .build();
        }

        static Iterable<SchemaRegistryFormat.IndexEntry> buildIndex(
            List<SchemaRegistryFormat.IndexEntry> index,
            SchemaRegistryFormat.PositionInfo position,
            long version
        ) {
            List<SchemaRegistryFormat.IndexEntry> entries = new ArrayList<>(index.size());
            Collections.copy(index, entries);
            entries.add(
                SchemaRegistryFormat.IndexEntry.newBuilder()
                    .setPosition(position)
                    .setVersion(version)
                    .build()
            );
            entries.sort(comparingLong(SchemaRegistryFormat.IndexEntry::getVersion));
            return entries;
        }
    }

    static class LocatorEntry {
        final SchemaRegistryFormat.SchemaLocator locator;
        final Integer version;

        LocatorEntry(SchemaRegistryFormat.SchemaLocator locator, Integer version) {
            this.locator = locator;
            this.version = version;
        }
    }
}
