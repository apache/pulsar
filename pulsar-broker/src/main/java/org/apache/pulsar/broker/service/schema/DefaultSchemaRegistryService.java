package org.apache.pulsar.broker.service.schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.schema.SchemaRegistryFormat;
import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;

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
    private static final String SCHEMA_PATH = "/schemas";
    private static final HashFunction HASH_FUNCTION = Hashing.sha1();

    private final PulsarService pulsar;
    private final ZooKeeperCache localZkCache;
    private final Clock clock;
    private BookKeeper bookKeeper;

    public static DefaultSchemaRegistryService create(PulsarService pulsar) throws KeeperException, InterruptedException {
        DefaultSchemaRegistryService service = new DefaultSchemaRegistryService(pulsar);
        service.init();
        return service;
    }

    @VisibleForTesting
    DefaultSchemaRegistryService(PulsarService pulsar, Clock clock) {
        this.pulsar = pulsar;
        this.localZkCache = pulsar.getLocalZkCache();
        this.clock = clock;
    }

    private DefaultSchemaRegistryService(PulsarService pulsar) {
        this(pulsar, Clock.systemUTC());
    }

    void init() throws KeeperException, InterruptedException {
        localZkCache.getZooKeeper().create(SCHEMA_PATH, new byte[]{}, Collections.emptyList(), CreateMode.PERSISTENT);
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
    public CompletableFuture<Schema> getSchema(String schemaId) {
        return getSchemaLocator(getSchemaPath(schemaId)).thenCompose(locator -> {

            if (!locator.isPresent()) {
                return completedFuture(null);
            }

            SchemaRegistryFormat.SchemaLocator schemaLocator = locator.get().locator;
            return readSchemaEntry(schemaLocator.getPosition())
                .thenApply(Functions::schemaEntryToSchema);
        });
    }

    @Override
    @NotNull
    public CompletableFuture<Schema> getSchema(String schemaId, long version) {
        return getSchemaLocator(getSchemaPath(schemaId)).thenCompose(locator -> {

            if (!locator.isPresent()) {
                return completedFuture(null);
            }

            SchemaRegistryFormat.SchemaLocator schemaLocator = locator.get().locator;
            if (version > schemaLocator.getInfo().getVersion()) {
                return completedFuture(null);
            }

            return findSchemaEntry(schemaLocator.getIndexList(), version)
                .thenApply(Functions::schemaEntryToSchema);
        });
    }

    @Override
    @NotNull
    public CompletableFuture<Long> putSchema(Schema schema) {
        return getOrCreateSchemaLocator(getSchemaPath(schema.id)).thenCompose(locatorEntry -> {
            SchemaRegistryFormat.SchemaLocator locator = locatorEntry.locator;

            return createLedger()
                .thenCompose(ledgerHandle ->
                    addEntry(ledgerHandle, newSchemaEntry(locator.getIndexList(), schema))
                        .thenCompose(position -> {
                                SchemaRegistryFormat.PositionInfo positionInfo =
                                    Functions.buildPositionInfo(ledgerHandle, position);
                                long latestVersion = locator.getInfo().getVersion();
                                long nextVersion = latestVersion + 1;
                                return putSchemaLocator(getSchemaPath(schema.id),
                                    SchemaRegistryFormat.SchemaLocator.newBuilder()
                                        .setInfo(Functions.buildSchemaInfo(schema))
                                        .setPosition(positionInfo)
                                        .addAllIndex(Functions.buildIndex(
                                            locator.getIndexList(),
                                            positionInfo,
                                            nextVersion)
                                        ).build(), locatorEntry.version
                                ).thenApply(ignore -> nextVersion);
                            }
                        )
                );
        });
    }

    @Override
    @NotNull
    public CompletableFuture<Void> deleteSchema(String schemaId, String user) {
        return getSchema(schemaId).thenCompose(schema -> {
            if (isNull(schema)) {
                return completedFuture(null);
            } else {
                return putSchema(
                    Schema.newBuilder()
                        .isDeleted(true)
                        .id(schema.id)
                        .data(new byte[]{})
                        .timestamp(clock.millis())
                        .type(SchemaType.NONE)
                        .user(user)
                        .version(schema.version + 1)
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
        return SCHEMA_PATH + "/" + schemaId;
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

    private CompletableFuture<Void> putSchemaLocator(String id, SchemaRegistryFormat.SchemaLocator schema, int version) {
        ZooKeeper zooKeeper = localZkCache.getZooKeeper();
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

                ZooKeeper zooKeeper = localZkCache.getZooKeeper();
                zooKeeper.create(schema, locator.toByteArray(), Collections.emptyList(), CreateMode.PERSISTENT,
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
                .id(entry.getInfo().getSchemaId())
                .timestamp(entry.getInfo().getTimestamp())
                .type(convertToDomainType(entry.getInfo().getType()))
                .user(entry.getInfo().getAddedBy())
                .version(entry.getInfo().getVersion())
                .build();
        }

        static SchemaRegistryFormat.SchemaEntry newSchemaEntry(
            List<SchemaRegistryFormat.IndexEntry> index,
            Schema schema
        ) {
            return SchemaRegistryFormat.SchemaEntry.newBuilder()
                .setInfo(SchemaRegistryFormat.SchemaInfo.newBuilder()
                    .setSchemaId(schema.id)
                    .setVersion(schema.version)
                    .setAddedBy(schema.user)
                    .setType(Functions.convertFromDomainType(schema.type))
                    .setHash(copyFrom(
                        HASH_FUNCTION.hashBytes(
                            schema.data
                        ).asBytes())
                    ).build())
                .setSchemaData(copyFrom(schema.data))
                .addAllIndex(index)
                .setIsDeleted(schema.isDeleted)
                .build();
        }

        static SchemaRegistryFormat.SchemaInfo buildSchemaInfo(Schema schema) {
            return SchemaRegistryFormat.SchemaInfo.newBuilder()
                .setAddedBy(schema.user)
                .setVersion(schema.version)
                .setSchemaId(schema.id)
                .setTimestamp(schema.timestamp)
                .setType(convertFromDomainType(schema.type))
                .build();
        }

        static SchemaRegistryFormat.PositionInfo buildPositionInfo(LedgerHandle ledgerHandle, long entryId) {
            return SchemaRegistryFormat.PositionInfo.newBuilder()
                .setLedgerId(ledgerHandle.getId())
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
