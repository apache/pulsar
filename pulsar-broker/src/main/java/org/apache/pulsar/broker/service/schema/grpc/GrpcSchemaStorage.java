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
package org.apache.pulsar.broker.service.schema.grpc;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.protocol.schema.StoredSchema;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.grpc.DeleteSchemaRequest;
import org.apache.pulsar.common.schema.grpc.GetAllVersionSchemaRequest;
import org.apache.pulsar.common.schema.grpc.GetAllVersionSchemaResponse;
import org.apache.pulsar.common.schema.grpc.GetVersionSchemaRequest;
import org.apache.pulsar.common.schema.grpc.GrpcSchemaStorageServiceGrpc;
import org.apache.pulsar.common.schema.grpc.GrpcSchemaVersion;
import org.apache.pulsar.common.schema.grpc.PutSchemaRequest;
import org.apache.pulsar.common.schema.grpc.VersionedSchemaResponse;

public class GrpcSchemaStorage implements SchemaStorage {

    private final ServiceConfiguration conf;
    private final Executor executor;
    private ManagedChannel channel;
    private GrpcSchemaStorageServiceGrpc.GrpcSchemaStorageServiceFutureStub stub;

    public GrpcSchemaStorage(PulsarService pulsar) {
        this.conf = pulsar.getConfiguration();
        this.executor = pulsar.getIoEventLoopGroup();
    }

    @Override
    public CompletableFuture<SchemaVersion> put(String key, byte[] value, byte[] hash) {
        PutSchemaRequest request = PutSchemaRequest.newBuilder()
                .setKey(key).setValue(ByteString.copyFrom(value)).setHash(ByteString.copyFrom(hash)).build();
        ListenableFuture<GrpcSchemaVersion> f = stub.put(request);
        return fromListenableFuture(f).thenApply(v -> BytesSchemaVersion.of(v.getSchemaVersion().toByteArray()));
    }

    @Override
    public CompletableFuture<StoredSchema> get(String key, SchemaVersion version) {
        GetVersionSchemaRequest request = GetVersionSchemaRequest.newBuilder()
                .setKey(key).setVersion(
                        GrpcSchemaVersion.newBuilder().setSchemaVersion(ByteString.copyFrom(version.bytes()))).build();

        ListenableFuture<VersionedSchemaResponse> f = stub.get(request);
        return fromListenableFuture(f).thenApply(v ->
                new StoredSchema(v.getData().toByteArray(),
                        BytesSchemaVersion.of(v.getVersion().getSchemaVersion().toByteArray())));
    }

    @Override
    public CompletableFuture<List<CompletableFuture<StoredSchema>>> getAll(String key) {
        GetAllVersionSchemaRequest request = GetAllVersionSchemaRequest
                .newBuilder().setKey(key).build();
        ListenableFuture<GetAllVersionSchemaResponse> future = stub.getAll(request);
        return fromListenableFuture(future).thenApply(values -> {
            if (values.getSchemasCount() == 0) {
                return List.of();
            }
            List<CompletableFuture<StoredSchema>> futures = new ArrayList<>();
            values.getSchemasList().forEach(schema -> {
                byte[] data = schema.getData().toByteArray();
                SchemaVersion version = BytesSchemaVersion.of(schema.getVersion().getSchemaVersion().toByteArray());
                futures.add(CompletableFuture.completedFuture(new StoredSchema(data, version)));
            });
            return futures;
        });
    }

    @Override
    public CompletableFuture<SchemaVersion> delete(String key, boolean forcefully) {
        DeleteSchemaRequest request = DeleteSchemaRequest.newBuilder()
                .setKey(key).setForce(forcefully).build();

        ListenableFuture<GrpcSchemaVersion> f = stub.delete(request);
        return fromListenableFuture(f).thenApply(v -> BytesSchemaVersion.of(v.getSchemaVersion().toByteArray()));
    }

    @Override
    public CompletableFuture<SchemaVersion> delete(String key) {
        return delete(key, false);
    }

    @Override
    public SchemaVersion versionFromBytes(byte[] version) {
        ByteBuffer bb = ByteBuffer.wrap(version);
        return new LongSchemaVersion(bb.getLong());
    }

    @Override
    public void start() throws Exception {
        String address = conf.getSchemaRegistryStorageGrpcEndpoint();
        channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
        stub = GrpcSchemaStorageServiceGrpc.newFutureStub(channel);
    }

    @Override
    public void close() throws Exception {
        if (null != stub) {
            stub = null;
        }
        if (null != channel) {
            channel.shutdown();
        }
    }

    private <T> CompletableFuture<T> fromListenableFuture(ListenableFuture<T> future) {
        CompletableFuture<T> ret = new CompletableFuture<>();
        future.addListener(() -> {
            try {
                T v = future.get();
                ret.complete(v);
            } catch (Throwable t) {
                ret.completeExceptionally(t);
            }
        }, executor);
        return ret;
    }
}
