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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.isNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.pulsar.broker.service.schema.SchemaRegistryServiceImpl.Functions.toPairs;
import static org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE;
import static org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy.FORWARD_TRANSITIVE;
import static org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy.FULL_TRANSITIVE;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.schema.exceptions.IncompatibleSchemaException;
import org.apache.pulsar.broker.service.schema.exceptions.NotExistSchemaException;
import org.apache.pulsar.broker.service.schema.exceptions.SchemaException;
import org.apache.pulsar.broker.service.schema.proto.SchemaRegistryFormat;
import org.apache.pulsar.broker.service.schema.validator.StructSchemaDataValidator;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaHash;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.protocol.schema.StoredSchema;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.FutureUtil;
import org.jspecify.annotations.NonNull;

@Slf4j
public class SchemaRegistryServiceImpl implements SchemaRegistryService {
    private static HashFunction hashFunction = Hashing.sha256();
    private final Map<SchemaType, SchemaCompatibilityCheck> compatibilityChecks;
    private final SchemaStorage schemaStorage;
    private final Clock clock;
    private final SchemaRegistryStats stats;

    @VisibleForTesting
    SchemaRegistryServiceImpl(SchemaStorage schemaStorage,
                              Map<SchemaType, SchemaCompatibilityCheck> compatibilityChecks,
                              Clock clock,
                              PulsarService pulsarService) {
        this.schemaStorage = schemaStorage;
        this.compatibilityChecks = compatibilityChecks;
        this.clock = clock;
        this.stats = new SchemaRegistryStats(pulsarService);
    }

    SchemaRegistryServiceImpl(SchemaStorage schemaStorage,
                              Map<SchemaType, SchemaCompatibilityCheck> compatibilityChecks,
                              PulsarService pulsarService) {
        this(schemaStorage, compatibilityChecks, Clock.systemUTC(), pulsarService);
    }

    @Override
    @NonNull
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId) {
        return getSchema(schemaId, SchemaVersion.Latest).thenApply((schema) -> {
            if (schema != null && schema.schema.isDeleted()) {
                return null;
            } else {
                return schema;
            }
        });
    }

    @Override
    @NonNull
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, SchemaVersion version) {
        long start = this.clock.millis();

        CompletableFuture<StoredSchema> completableFuture;
        if (version == SchemaVersion.Latest) {
            completableFuture = schemaStorage.get(schemaId, version);
        } else {
            long longVersion = ((LongSchemaVersion) version).getVersion();
            //If the schema has been deleted, it cannot be obtained
            completableFuture = trimDeletedSchemaAndGetList(schemaId)
                .thenApply(metadataList -> metadataList.stream().filter(schemaAndMetadata ->
                        ((LongSchemaVersion) schemaAndMetadata.version).getVersion() == longVersion)
                        .collect(Collectors.toList())
                ).thenCompose(metadataList -> {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Meta data list size {}", schemaId,
                                    CollectionUtils.isEmpty(metadataList) ? 0 : metadataList.size());
                        }
                        if (CollectionUtils.isNotEmpty(metadataList)) {
                            return schemaStorage.get(schemaId, version);
                        }
                        return completedFuture(null);
                    }
                );
        }

        return completableFuture
                .thenCompose(stored -> {
                    if (isNull(stored)) {
                        return completedFuture(null);
                    } else {
                        return Functions.bytesToSchemaInfo(stored.data)
                                .thenApply(Functions::schemaInfoToSchema)
                                .thenApply(schema -> new SchemaAndMetadata(schemaId, schema, stored.version));
                    }
                })
                .whenComplete((v, t) -> {
                    var latencyMs = this.clock.millis() - start;
                    if (t != null) {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Get schema failed", schemaId);
                        }
                        this.stats.recordGetFailed(schemaId, latencyMs);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug(null == v ? "[{}] Schema not found" : "[{}] Schema is present", schemaId);
                        }
                        this.stats.recordGetLatency(schemaId, latencyMs);
                    }
                });
    }

    @Override
    public CompletableFuture<List<CompletableFuture<SchemaAndMetadata>>> getAllSchemas(String schemaId) {
        long start = this.clock.millis();

        return schemaStorage.getAll(schemaId)
                .thenCompose(schemas -> convertToSchemaAndMetadata(schemaId, schemas))
                .whenComplete((v, t) -> {
                    var latencyMs = this.clock.millis() - start;
                    if (t != null) {
                        this.stats.recordListFailed(schemaId, latencyMs);
                    } else {
                        this.stats.recordListLatency(schemaId, latencyMs);
                    }
                });
    }

    private CompletableFuture<List<CompletableFuture<SchemaAndMetadata>>> convertToSchemaAndMetadata(String schemaId,
             List<CompletableFuture<StoredSchema>> schemas) {
        List<CompletableFuture<SchemaAndMetadata>> list = schemas.stream()
                .map(future -> future.thenCompose(stored ->
                        Functions.bytesToSchemaInfo(stored.data)
                                .thenApply(Functions::schemaInfoToSchema)
                                .thenApply(schema ->
                                        new SchemaAndMetadata(schemaId, schema, stored.version)
                                )
                ))
                .collect(Collectors.toList());
        if (log.isDebugEnabled()) {
            log.debug("[{}] {} schemas is found", schemaId, list.size());
        }
        return CompletableFuture.completedFuture(list);
    }

    @Override
    @NonNull
    public CompletableFuture<SchemaVersion> putSchemaIfAbsent(String schemaId, SchemaData schema,
                                                              SchemaCompatibilityStrategy strategy) {
        MutableLong start = new MutableLong(0);
        CompletableFuture<SchemaVersion> promise = new CompletableFuture<>();
        schemaStorage.put(schemaId,
            schemasFuture -> schemasFuture
                .thenCompose(schemaFutureList -> trimDeletedSchemaAndGetList(schemaId,
                        convertToSchemaAndMetadata(schemaId, schemaFutureList)))
                .thenCompose(schemaAndMetadataList -> getSchemaVersionBySchemaData(schemaAndMetadataList, schema)
                    .thenCompose(schemaVersion -> {
                        if (schemaVersion != null) {
                            if (log.isDebugEnabled()) {
                                log.debug("[{}] Schema is already exists", schemaId);
                            }
                            promise.complete(schemaVersion);
                            return CompletableFuture.completedFuture(null);
                        }
                        CompletableFuture<Void> checkCompatibilityFuture = new CompletableFuture<>();
                        if (schemaAndMetadataList.size() != 0) {
                            if (isTransitiveStrategy(strategy)) {
                                checkCompatibilityFuture =
                                        checkCompatibilityWithAll(schemaId, schema, strategy, schemaAndMetadataList);
                            } else {
                                checkCompatibilityFuture = checkCompatibilityWithLatest(schemaId, schema, strategy);
                            }
                        } else {
                            checkCompatibilityFuture.complete(null);
                        }
                        return checkCompatibilityFuture.thenCompose(v -> {
                            byte[] context = hashFunction.hashBytes(schema.getData()).asBytes();
                            SchemaRegistryFormat.SchemaInfo info = SchemaRegistryFormat.SchemaInfo.newBuilder()
                                    .setType(Functions.convertFromDomainType(schema.getType()))
                                    .setSchema(ByteString.copyFrom(schema.getData()))
                                    .setSchemaId(schemaId)
                                    .setUser(schema.getUser())
                                    .setDeleted(false)
                                    .setTimestamp(clock.millis())
                                    .addAllProps(toPairs(schema.getProps()))
                                    .build();

                            start.setValue(this.clock.millis());
                            return CompletableFuture.completedFuture(Pair.of(info.toByteArray(), context));
                        });
                }))).whenComplete((v, ex) -> {
                    var latencyMs = this.clock.millis() - start.getValue();
                    if (ex != null) {
                        log.error("[{}] Put schema failed", schemaId, ex);
                        if (start.getValue() != 0) {
                            this.stats.recordPutFailed(schemaId, latencyMs);
                        }
                        promise.completeExceptionally(ex);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Put schema finished", schemaId);
                        }
                        // The schema storage will return null schema version if no schema is persisted to the storage
                        if (v != null) {
                            promise.complete(v);
                            if (start.getValue() != 0) {
                                this.stats.recordPutLatency(schemaId, this.clock.millis() - start.getValue());
                            }
                        }
                    }
            });
        return promise;
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchema(String schemaId, String user, boolean force) {
        long start = this.clock.millis();

        if (force) {
            return deleteSchemaStorage(schemaId, true);
        }
        byte[] deletedEntry = deleted(schemaId, user).toByteArray();
        return schemaStorage
                .put(schemaId, deletedEntry, new byte[]{})
                .whenComplete((v, t) -> {
                    var latencyMs = this.clock.millis() - start;
                    if (t != null) {
                        log.error("[{}] User {} delete schema failed", schemaId, user);
                        this.stats.recordDelFailed(schemaId, latencyMs);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] User {} delete schema finished", schemaId, user);
                        }
                        this.stats.recordDelLatency(schemaId, latencyMs);
                    }
                });
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchemaStorage(String schemaId) {
        return deleteSchemaStorage(schemaId, false);
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchemaStorage(String schemaId, boolean forcefully) {
        long start = this.clock.millis();

        return schemaStorage.delete(schemaId, forcefully)
                .whenComplete((v, t) -> {
                    var latencyMs = this.clock.millis() - start;
                    if (t != null) {
                        this.stats.recordDelFailed(schemaId, latencyMs);
                        log.error("[{}] Delete schema storage failed", schemaId);
                    } else {
                        this.stats.recordDelLatency(schemaId, latencyMs);
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Delete schema storage finished", schemaId);
                        }
                    }
                });
    }

    @Override
    public CompletableFuture<Boolean> isCompatible(String schemaId, SchemaData schema,
                                                   SchemaCompatibilityStrategy strategy) {
        return checkCompatible(schemaId, schema, strategy).thenApply(v -> true);
    }

    private static boolean isTransitiveStrategy(SchemaCompatibilityStrategy strategy) {
        if (FORWARD_TRANSITIVE.equals(strategy)
                || BACKWARD_TRANSITIVE.equals(strategy)
                || FULL_TRANSITIVE.equals(strategy)) {
            return true;
        }
        return false;
    }

    @Override
    public CompletableFuture<Void> checkCompatible(String schemaId, SchemaData schema,
                                                                    SchemaCompatibilityStrategy strategy) {
        switch (strategy) {
            case FORWARD_TRANSITIVE:
            case BACKWARD_TRANSITIVE:
            case FULL_TRANSITIVE:
                return checkCompatibilityWithAll(schemaId, schema, strategy);
            default:
                return checkCompatibilityWithLatest(schemaId, schema, strategy);
        }
    }

    @Override
    public SchemaVersion versionFromBytes(byte[] version) {
        return schemaStorage.versionFromBytes(version);
    }

    @Override
    public void close() throws Exception {
        schemaStorage.close();
        this.stats.close();
    }

    private SchemaRegistryFormat.SchemaInfo deleted(String schemaId, String user) {
        return SchemaRegistryFormat.SchemaInfo.newBuilder()
            .setSchemaId(schemaId)
            .setType(SchemaRegistryFormat.SchemaInfo.SchemaType.NONE)
            .setSchema(ByteString.EMPTY)
            .setUser(user)
            .setDeleted(true)
            .setTimestamp(clock.millis())
            .build();
    }

    private void checkCompatible(SchemaAndMetadata existingSchema, SchemaData newSchema,
                                 SchemaCompatibilityStrategy strategy) throws IncompatibleSchemaException {
        SchemaData existingSchemaData = existingSchema.schema;
        if (newSchema.getType() != existingSchemaData.getType()) {
            throw new IncompatibleSchemaException(String.format("Incompatible schema: "
                            + "exists schema type %s, new schema type %s",
                    existingSchemaData.getType(), newSchema.getType()));
        }
        SchemaHash existingHash = SchemaHash.of(existingSchemaData);
        SchemaHash newHash = SchemaHash.of(newSchema);
        if (!newHash.equals(existingHash)) {
            compatibilityChecks.getOrDefault(newSchema.getType(), SchemaCompatibilityCheck.DEFAULT)
                    .checkCompatible(existingSchemaData, newSchema, strategy);
        }
    }

    public CompletableFuture<Long> findSchemaVersion(String schemaId, SchemaData schemaData) {
        return trimDeletedSchemaAndGetList(schemaId)
                .thenCompose(schemaAndMetadataList -> {
                    SchemaHash newHash = SchemaHash.of(schemaData);
                    for (SchemaAndMetadata schemaAndMetadata : schemaAndMetadataList) {
                        if (newHash.equals(SchemaHash.of(schemaAndMetadata.schema))) {
                            return completedFuture(((LongSchemaVersion) schemaStorage
                                    .versionFromBytes(schemaAndMetadata.version.bytes())).getVersion());
                        }
                    }
                    return completedFuture(NO_SCHEMA_VERSION);
                });
    }

    @Override
    public CompletableFuture<Void> checkConsumerCompatibility(String schemaId, SchemaData schemaData,
                                                              SchemaCompatibilityStrategy strategy) {
        if (SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE == strategy) {
            return CompletableFuture.completedFuture(null);
        }
        return getSchema(schemaId).thenCompose(existingSchema -> {
            if (existingSchema != null && !existingSchema.schema.isDeleted()) {
                if (strategy == SchemaCompatibilityStrategy.BACKWARD
                        || strategy == SchemaCompatibilityStrategy.FORWARD
                        || strategy == SchemaCompatibilityStrategy.FORWARD_TRANSITIVE
                        || strategy == SchemaCompatibilityStrategy.FULL) {
                    return checkCompatibilityWithLatest(schemaId, schemaData, SchemaCompatibilityStrategy.BACKWARD);
                } else {
                    return checkCompatibilityWithAll(schemaId, schemaData, strategy);
                }
            } else {
                return FutureUtil.failedFuture(new NotExistSchemaException("Topic does not have schema to check"));
            }
        });
    }

    @Override
    public CompletableFuture<SchemaVersion> getSchemaVersionBySchemaData(
            List<SchemaAndMetadata> schemaAndMetadataList,
            SchemaData schemaData) {
        if (schemaAndMetadataList == null || schemaAndMetadataList.size() == 0) {
            return CompletableFuture.completedFuture(null);
        }
        final CompletableFuture<SchemaVersion> completableFuture = new CompletableFuture<>();
        SchemaVersion schemaVersion;
        if (isUsingAvroSchemaParser(schemaData.getType())) {
            Schema.Parser parser = new Schema.Parser(StructSchemaDataValidator.COMPATIBLE_NAME_VALIDATOR);
            Schema newSchema = parser.parse(new String(schemaData.getData(), UTF_8));

            for (SchemaAndMetadata schemaAndMetadata : schemaAndMetadataList) {
                if (isUsingAvroSchemaParser(schemaAndMetadata.schema.getType())) {
                    Schema.Parser existParser =
                            new Schema.Parser(StructSchemaDataValidator.COMPATIBLE_NAME_VALIDATOR);
                    Schema existSchema = existParser.parse(new String(schemaAndMetadata.schema.getData(), UTF_8));
                    if (newSchema.equals(existSchema) && schemaAndMetadata.schema.getType() == schemaData.getType()) {
                        schemaVersion = schemaAndMetadata.version;
                        completableFuture.complete(schemaVersion);
                        return completableFuture;
                    }
                } else {
                    if (Arrays.equals(hashFunction.hashBytes(schemaAndMetadata.schema.getData()).asBytes(),
                            hashFunction.hashBytes(schemaData.getData()).asBytes())
                            && schemaAndMetadata.schema.getType() == schemaData.getType()) {
                        schemaVersion = schemaAndMetadata.version;
                        completableFuture.complete(schemaVersion);
                        return completableFuture;
                    }
                }
            }
        } else {
            for (SchemaAndMetadata schemaAndMetadata : schemaAndMetadataList) {
                if (Arrays.equals(hashFunction.hashBytes(schemaAndMetadata.schema.getData()).asBytes(),
                        hashFunction.hashBytes(schemaData.getData()).asBytes())
                        && schemaAndMetadata.schema.getType() == schemaData.getType()) {
                    schemaVersion = schemaAndMetadata.version;
                    completableFuture.complete(schemaVersion);
                    return completableFuture;
                }
            }
        }
        completableFuture.complete(null);
        return completableFuture;
    }

    private CompletableFuture<Void> checkCompatibilityWithLatest(String schemaId, SchemaData schema,
                                                                    SchemaCompatibilityStrategy strategy) {
        if (SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE == strategy) {
            return CompletableFuture.completedFuture(null);
        }
        return getSchema(schemaId).thenCompose(existingSchema -> {
            if (existingSchema != null && !existingSchema.schema.isDeleted()) {
                CompletableFuture<Void> result = new CompletableFuture<>();
                result.whenComplete((__, t) -> {
                    if (t != null) {
                        log.error("[{}] Schema is incompatible", schemaId);
                        this.stats.recordSchemaIncompatible(schemaId);
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("[{}] Schema is compatible", schemaId);
                        }
                        this.stats.recordSchemaCompatible(schemaId);
                    }
                });

                try {
                    checkCompatible(existingSchema, schema, strategy);
                    result.complete(null);
                } catch (IncompatibleSchemaException e) {
                    result.completeExceptionally(e);
                }
                return result;
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    private CompletableFuture<Void> checkCompatibilityWithAll(String schemaId, SchemaData schema,
                                                                     SchemaCompatibilityStrategy strategy) {

        return trimDeletedSchemaAndGetList(schemaId).thenCompose(schemaAndMetadataList ->
                checkCompatibilityWithAll(schemaId, schema, strategy, schemaAndMetadataList));
    }

    private CompletableFuture<Void> checkCompatibilityWithAll(String schemaId, SchemaData schema,
                                                              SchemaCompatibilityStrategy strategy,
                                                              List<SchemaAndMetadata> schemaAndMetadataList) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        result.whenComplete((v, t) -> {
            if (t != null) {
                this.stats.recordSchemaIncompatible(schemaId);
                log.error("[{}] Schema is incompatible, schema type {}", schemaId, schema.getType());
            } else {
                this.stats.recordSchemaCompatible(schemaId);
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Schema is compatible, schema type {}", schemaId, schema.getType());
                }
            }
        });

        if (strategy == SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE) {
            result.complete(null);
        } else {
            SchemaAndMetadata breakSchema = null;
            for (SchemaAndMetadata schemaAndMetadata : schemaAndMetadataList) {
                if (schemaAndMetadata.schema.getType() != schema.getType()) {
                    breakSchema = schemaAndMetadata;
                    break;
                }
            }
            if (breakSchema == null) {
                try {
                    compatibilityChecks.getOrDefault(schema.getType(), SchemaCompatibilityCheck.DEFAULT)
                            .checkCompatible(schemaAndMetadataList
                                    .stream()
                                    .map(schemaAndMetadata -> schemaAndMetadata.schema)
                                    .collect(Collectors.toList()), schema, strategy);
                    result.complete(null);
                } catch (Exception e) {
                    if (e instanceof IncompatibleSchemaException) {
                        result.completeExceptionally(e);
                    } else {
                        result.completeExceptionally(new IncompatibleSchemaException(e));
                    }
                }
            } else {
                result.completeExceptionally(new IncompatibleSchemaException(
                        String.format("Incompatible schema: exists schema type %s, new schema type %s",
                                breakSchema.schema.getType(), schema.getType())));
            }
        }
        return result;
    }

    public CompletableFuture<List<SchemaAndMetadata>> trimDeletedSchemaAndGetList(String schemaId) {
        CompletableFuture<List<CompletableFuture<SchemaAndMetadata>>> schemaFutureList = getAllSchemas(schemaId);
        return trimDeletedSchemaAndGetList(schemaId, schemaFutureList);
    }

    private CompletableFuture<List<SchemaAndMetadata>> trimDeletedSchemaAndGetList(String schemaId,
                CompletableFuture<List<CompletableFuture<SchemaAndMetadata>>> schemaFutureList) {
        CompletableFuture<List<SchemaAndMetadata>> schemaResult = new CompletableFuture<>();
        schemaFutureList.thenCompose(FutureUtils::collect).handle((schemaList, ex) -> {
            List<SchemaAndMetadata> list = ex != null ? new ArrayList<>() : schemaList;
            if (ex != null) {
                final Throwable rc = FutureUtil.unwrapCompletionException(ex);
                boolean recoverable = !(rc instanceof SchemaException) || ((SchemaException) rc).isRecoverable();
                // if error is recoverable then fail the request.
                if (recoverable) {
                    schemaResult.completeExceptionally(rc);
                    return null;
                }
                // clean the schema list for recoverable and delete the schema from zk
                schemaFutureList.getNow(Collections.emptyList()).forEach(schemaFuture -> {
                    if (!schemaFuture.isCompletedExceptionally()) {
                        list.add(schemaFuture.getNow(null));
                        return;
                    }
                });
                trimDeletedSchemaAndGetList(list);
                // clean up the broken schema from zk
                deleteSchemaStorage(schemaId, true).handle((sv, th) -> {
                    log.info("Clean up non-recoverable schema {}. Deletion of schema {} {}", rc.getMessage(),
                            schemaId, (th == null ? "successful" : "failed, " + th.getCause().getMessage()));
                    schemaResult.complete(list);
                    return null;
                });
                return null;
            }
            // trim the deleted schema and return the result if schema is retrieved successfully
            List<SchemaAndMetadata> trimmed = trimDeletedSchemaAndGetList(list);
            schemaResult.complete(trimmed);
            return null;
        });
        return schemaResult;
    }

    private List<SchemaAndMetadata> trimDeletedSchemaAndGetList(List<SchemaAndMetadata> list) {
        // Trim the prefix of schemas before the latest delete.
        int lastIndex = list.size() - 1;
        for (int i = lastIndex; i >= 0; i--) {
            if (list.get(i).schema.isDeleted()) {
                if (i == lastIndex) { // if the latest schema is a delete, there's no schemas to compare
                    return Collections.emptyList();
                } else {
                    return list.subList(i + 1, list.size());
                }
            }
        }
        return list;
    }

    interface Functions {
        static SchemaType convertToDomainType(SchemaRegistryFormat.SchemaInfo.SchemaType type) {
            if (type.getNumber() < 0) {
                return SchemaType.NONE;
            } else {
                // the value of type in `SchemaType` is always 1 less than the value of type `SchemaInfo.SchemaType`
                return SchemaType.valueOf(type.getNumber() - 1);
            }
        }

        static SchemaRegistryFormat.SchemaInfo.SchemaType convertFromDomainType(SchemaType type) {
            if (type.getValue() < 0) {
                return SchemaRegistryFormat.SchemaInfo.SchemaType.NONE;
            } else {
                return SchemaRegistryFormat.SchemaInfo.SchemaType.valueOf(type.getValue() + 1);
            }
        }

        static Map<String, String> toMap(List<SchemaRegistryFormat.SchemaInfo.KeyValuePair> pairs) {
            Map<String, String> map = new HashMap<>();
            for (SchemaRegistryFormat.SchemaInfo.KeyValuePair pair : pairs) {
                map.put(pair.getKey(), pair.getValue());
            }
            return map;
        }

        static List<SchemaRegistryFormat.SchemaInfo.KeyValuePair> toPairs(Map<String, String> map) {
            if (isNull(map)) {
                return Collections.emptyList();
            }
            List<SchemaRegistryFormat.SchemaInfo.KeyValuePair> pairs = new ArrayList<>(map.size());
            for (Map.Entry<String, String> entry : map.entrySet()) {
                SchemaRegistryFormat.SchemaInfo.KeyValuePair.Builder builder =
                    SchemaRegistryFormat.SchemaInfo.KeyValuePair.newBuilder();
                pairs.add(builder.setKey(entry.getKey()).setValue(entry.getValue()).build());
            }
            return pairs;
        }

        static SchemaData schemaInfoToSchema(SchemaRegistryFormat.SchemaInfo info) {
            return SchemaData.builder()
                .user(info.getUser())
                .type(convertToDomainType(info.getType()))
                .data(info.getSchema().toByteArray())
                .timestamp(info.getTimestamp())
                .isDeleted(info.getDeleted())
                .props(toMap(info.getPropsList()))
                .build();
        }

        static CompletableFuture<SchemaRegistryFormat.SchemaInfo> bytesToSchemaInfo(byte[] bytes) {
            CompletableFuture<SchemaRegistryFormat.SchemaInfo> future;
            try {
                future = completedFuture(SchemaRegistryFormat.SchemaInfo.parseFrom(bytes));
            } catch (InvalidProtocolBufferException e) {
                future = new CompletableFuture<>();
                future.completeExceptionally(e);
            }
            return future;
        }
    }

    public static boolean isUsingAvroSchemaParser(SchemaType type) {
        switch (type) {
            case AVRO:
            case JSON:
            case PROTOBUF:
                return true;
            default:
                return false;
        }
    }

}
