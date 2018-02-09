/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service.schema;

import static java.util.concurrent.CompletableFuture.completedFuture;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import javax.validation.constraints.NotNull;
import org.apache.pulsar.broker.schema.SchemaRegistryFormat;
import org.apache.pulsar.common.schema.Schema;
import org.apache.pulsar.common.schema.SchemaType;

public class SchemaRegistryServiceImpl implements SchemaRegistryService {
    private final SchemaStorage schemaStorage;
    private final Clock clock;

    @VisibleForTesting
    SchemaRegistryServiceImpl(SchemaStorage schemaStorage, Clock clock) {
        this.schemaStorage = schemaStorage;
        this.clock = clock;
    }

    @VisibleForTesting
    SchemaRegistryServiceImpl(SchemaStorage schemaStorage) {
        this(schemaStorage, Clock.systemUTC());
    }

    @Override
    @NotNull
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId) {
        return getSchema(schemaId, SchemaVersion.Latest);
    }

    @Override
    @NotNull
    public CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, long version) {
        return getSchema(schemaId, SchemaVersion.fromLong(version));
    }

    private CompletableFuture<SchemaAndMetadata> getSchema(String schemaId, SchemaVersion version) {
        return schemaStorage.get(schemaId, version).thenCompose(stored ->
            Functions.bytesToSchemaInfo(stored.data)
                .thenApply(info -> Functions.schemaInfoToSchema(info, stored.version.toLong()))
                .thenApply(schema -> new SchemaAndMetadata(schemaId, schema, stored.version.toLong(), stored.metadata))
        );
    }

    @Override
    @NotNull
    public CompletableFuture<Long> putSchema(String schemaId, Schema schema) {
        SchemaRegistryFormat.SchemaInfo info = SchemaRegistryFormat.SchemaInfo.newBuilder()
            .setType(Functions.convertFromDomainType(schema.type))
            .setSchema(ByteString.copyFrom(schema.data))
            .setSchemaId(schemaId)
            .setUser(schema.user)
            .setDeleted(false)
            .setTimestamp(clock.millis())
            .build();
        return schemaStorage.put(schemaId, info.toByteArray())
            .thenApply(SchemaVersion::toLong);
    }

    @Override
    @NotNull
    public CompletableFuture<Long> deleteSchema(String schemaId, String user) {
        byte[] deletedEntry = deleted(schemaId, user).toByteArray();
        return schemaStorage.put(schemaId, deletedEntry)
            .thenApply(SchemaVersion::toLong);
    }

    @Override
    public void close() throws Exception {
        schemaStorage.close();
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

    interface Functions {
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

        static Schema schemaInfoToSchema(SchemaRegistryFormat.SchemaInfo info, long version) {
            return Schema.newBuilder()
                .user(info.getUser())
                .type(convertToDomainType(info.getType()))
                .data(info.getSchema().toByteArray())
                .version(version)
                .isDeleted(info.getDeleted())
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

}
