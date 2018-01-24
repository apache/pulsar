package org.apache.pulsar.broker.service.schema;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.schema.SchemaRegistryFormat;
import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.protobuf.ByteString.copyFrom;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.nonNull;
import static org.apache.pulsar.broker.service.schema.DefaultSchemaRegistryService.Functions.newSchemaEntry;
import static org.apache.pulsar.client.util.FutureUtil.completedFuture;

public class DefaultSchemaRegistryService implements SchemaRegistryService {
    private static final String SCHEMA_PATH = "/schemas/";
    private static final HashFunction HASH_FUNCTION = Hashing.sha1();

    private final PulsarService pulsar;
    private final ZooKeeperCache localZkCache;
    private BookKeeper bookKeeper;
    private Clock clock = Clock.systemUTC();

    public static DefaultSchemaRegistryService create(PulsarService pulsar) {
        return new DefaultSchemaRegistryService(pulsar);
    }

    private DefaultSchemaRegistryService(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.localZkCache = pulsar.getLocalZkCache();
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
        return getSchemaLocator(schemaId).thenCompose(locator -> {

            if (!locator.isPresent()) {
                return completedFuture(null);
            }

            SchemaRegistryFormat.SchemaLocator schemaLocator = locator.get().getKey();
            return readSchemaEntry(schemaLocator.getPosition())
                .thenApply(Functions::schemaEntryToSchema);
        });
    }

    @Override
    @NotNull
    public CompletableFuture<Schema> getSchema(String schemaId, long version) {
        return getSchemaLocator(schemaId).thenCompose(locator -> {

            if (!locator.isPresent()) {
                return completedFuture(null);
            }

            SchemaRegistryFormat.SchemaLocator schemaLocator = locator.get().getKey();
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
        return getSchemaLocator(schema.id).thenCompose(optionalSchemaLocator -> {
            if (!optionalSchemaLocator.isPresent()) {
                return completedFuture(-1L);
            }

            Entry<SchemaRegistryFormat.SchemaLocator, Stat> locatorEntry = optionalSchemaLocator.get();
            SchemaRegistryFormat.SchemaLocator locator = locatorEntry.getKey();

            return createLedger()
                .thenCompose(ledgerHandle -> addEntry(ledgerHandle, newSchemaEntry(locator, schema))
                    .thenCompose(position -> {
                            SchemaRegistryFormat.PositionInfo positionInfo =
                                Functions.buildPositionInfo(ledgerHandle, position);
                            long latestVersion = locator.getInfo().getVersion();
                            long nextVersion = latestVersion + 1;
                            return putSchemaLocator(schema.id,
                                SchemaRegistryFormat.SchemaLocator.newBuilder()
                                    .setInfo(Functions.buildSchemaInfo(schema))
                                    .setPosition(positionInfo)
                                    .addAllIndex(Functions.buildIndex(locator, positionInfo, nextVersion))
                                    .build(), locatorEntry.getValue().getVersion()
                            ).thenApply(ignore -> nextVersion);
                        }
                    )
                );
        });
    }

    @Override
    @NotNull
    public CompletableFuture<Long> deleteSchema(String schemaId, String user) {
        return getSchema(schemaId).thenCompose(schema ->
            putSchema(
                Schema.newBuilder()
                    .isDeleted(true)
                    .id(schema.id)
                    .data(new byte[]{})
                    .timestamp(clock.millis())
                    .type(null)
                    .user(user)
                    .version(schema.version + 1)
                    .build()
            )
        );
    }

    @Override
    public void close() throws Exception {
        if (nonNull(bookKeeper)) {
            bookKeeper.close();
        }
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
            .thenCompose((ledger) -> Functions.parseSchemaEntry(ledger, position.getEntryId()))
            .thenCompose(Functions::parseSchemaEntry);
    }

    private CompletableFuture<Void> putSchemaLocator(String id, SchemaRegistryFormat.SchemaLocator schema, int version) {
        ZooKeeper zooKeeper = localZkCache.getZooKeeper();
        CompletableFuture<Void> future = new CompletableFuture<>();
        zooKeeper.setData(SCHEMA_PATH + id, schema.toByteArray(), version, (rc, path, ctx, stat) -> {
            KeeperException.Code code = KeeperException.Code.get(rc);
            if (code != KeeperException.Code.OK) {
                future.completeExceptionally(new Exception());
            } else {
                future.complete(null);
            }
        }, null);
        return future;
    }

    private CompletableFuture<Optional<Entry<SchemaRegistryFormat.SchemaLocator, Stat>>> getSchemaLocator(String schema) {
        return localZkCache.getEntryAsync(SCHEMA_PATH + schema, new SchemaLocatorDeserializer());
    }

    private CompletableFuture<Long> addEntry(LedgerHandle ledgerHandle, SchemaRegistryFormat.SchemaEntry entry) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        ledgerHandle.asyncAddEntry(entry.toByteArray(),
            (i, ledgerHandle1, l, o) -> future.complete(l), null
        );
        return future;
    }

    private CompletableFuture<LedgerHandle> createLedger() {
        final CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        bookKeeper.asyncCreateLedger(0, 0, DigestType.MAC, null,
            (i, ledgerHandle, o) -> future.complete(ledgerHandle), null
        );
        return future;
    }

    private CompletableFuture<LedgerHandle> openLedger(Long ledgerId) {
        final CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        bookKeeper.asyncOpenLedger(ledgerId, DigestType.MAC, null,
            (i, ledgerHandle, o) -> future.complete(ledgerHandle), null
        );
        return future;
    }

    static class SchemaLocatorDeserializer implements ZooKeeperCache.Deserializer<SchemaRegistryFormat.SchemaLocator> {
        @Override
        public SchemaRegistryFormat.SchemaLocator deserialize(String key, byte[] content) throws Exception {
            return SchemaRegistryFormat.SchemaLocator.parseFrom(content);
        }
    }

    interface Functions {
        static CompletableFuture<LedgerEntry> parseSchemaEntry(LedgerHandle ledger, long entry) {
            final CompletableFuture<LedgerEntry> future = new CompletableFuture<>();
            ledger.asyncReadEntries(entry, entry,
                (i, ledgerHandle, enumeration, o) -> future.complete(enumeration.nextElement()), null
            );
            return future;
        }

        static CompletableFuture<SchemaRegistryFormat.SchemaEntry> parseSchemaEntry(LedgerEntry ledgerEntry) {
            CompletableFuture<SchemaRegistryFormat.SchemaEntry> result = new CompletableFuture<>();
            try {
                result.complete(SchemaRegistryFormat.SchemaEntry.parseFrom(ledgerEntry.getEntryInputStream()));
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
                    return null;
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
                    return null;
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
            SchemaRegistryFormat.SchemaLocator locator,
            Schema schema
        ) {
            return SchemaRegistryFormat.SchemaEntry.newBuilder()
                .setInfo(SchemaRegistryFormat.SchemaInfo.newBuilder()
                    .setSchemaId(schema.id)
                    .setVersion(schema.version)
                    .setAddedBy(schema.user)
                    .setHash(copyFrom(
                        HASH_FUNCTION.hashBytes(
                            schema.data
                        ).asBytes())
                    ).build())
                .setSchemaData(copyFrom(schema.data))
                .addAllIndex(locator.getIndexList())
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
            SchemaRegistryFormat.SchemaLocator locator,
            SchemaRegistryFormat.PositionInfo position,
            long version
        ) {
            List<SchemaRegistryFormat.IndexEntry> entries = locator.getIndexList();
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
}
