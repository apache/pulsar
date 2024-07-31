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

import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import io.netty.channel.nio.NioEventLoopGroup;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.schema.grpc.GrpcSchemaStorageFactory;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.protocol.schema.StoredSchema;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.grpc.*;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class GrpcSchemaStorageTest {

    private Server server;
    private NioEventLoopGroup eventLoopGroup;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        server = Grpc.newServerBuilderForPort(8005, InsecureServerCredentials.create())
                .addService(new GrpcSchemaStorageServer())
                .build().start();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        server.shutdown();
        eventLoopGroup.shutdownGracefully();
    }

    private SchemaStorage buildSchemaStorage() throws Exception {
        PulsarService pulsar = Mockito.mock(PulsarService.class);
        eventLoopGroup = new NioEventLoopGroup();
        Mockito.doReturn(eventLoopGroup).when(pulsar).getIoEventLoopGroup();
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setSchemaRegistryStorageGrpcEndpoint("localhost:8005");
        conf.setSchemaRegistryStorageGrpcEnableTls(false);
        Mockito.doReturn(conf).when(pulsar).getConfig();
        Mockito.doReturn(conf).when(pulsar).getConfiguration();
        GrpcSchemaStorageFactory factory = new GrpcSchemaStorageFactory();
        SchemaStorage storage =  factory.create(pulsar);
        storage.start();
        return storage;
    }

    private byte[] putSchema(SchemaStorage schemaStorage) throws Exception {
        byte[] value = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
        CompletableFuture<SchemaVersion> version = schemaStorage.put("key1", value, new byte[0]);
        SchemaVersion result = version.get();
        long version0 = ((LongSchemaVersion) schemaStorage.versionFromBytes(result.bytes())).getVersion();
        Assert.assertEquals(version0, 1);

        version = schemaStorage.put("key1", value, new byte[0]);
        result = version.get();
        version0 = ((LongSchemaVersion) schemaStorage.versionFromBytes(result.bytes())).getVersion();
        Assert.assertEquals(version0, 2);

        version = schemaStorage.put("key1", value, new byte[0]);
        result = version.get();
        version0 = ((LongSchemaVersion) schemaStorage.versionFromBytes(result.bytes())).getVersion();
        Assert.assertEquals(version0, 3);
        return value;
    }

    @Test
    public void testPutSchema() throws Exception {
        SchemaStorage schemaStorage = buildSchemaStorage();
        try {
            putSchema(schemaStorage);
        } finally {
            schemaStorage.close();
        }
    }

    @Test
    public void testGetVersionSchema() throws Exception {
        SchemaStorage schemaStorage = buildSchemaStorage();
        try {
            byte[] value = putSchema(schemaStorage);
            CompletableFuture<StoredSchema> schema = schemaStorage.get("key1", new LongSchemaVersion(1));
            StoredSchema result = schema.get();
            Assert.assertEquals(new String(value), new String(result.data));
            Assert.assertEquals(schemaStorage.versionFromBytes(result.version.bytes()), new LongSchemaVersion(1));

            schema = schemaStorage.get("key1", new LongSchemaVersion(2));
            result = schema.get();
            Assert.assertEquals(new String(value), new String(result.data));
            Assert.assertEquals(schemaStorage.versionFromBytes(result.version.bytes()), new LongSchemaVersion(2));

            schema = schemaStorage.get("key1", new LongSchemaVersion(3));
            result = schema.get();
            Assert.assertEquals(new String(value), new String(result.data));
            Assert.assertEquals(schemaStorage.versionFromBytes(result.version.bytes()), new LongSchemaVersion(3));
        } finally {
            schemaStorage.close();
        }
    }

    @Test
    public void testGetAllSchemaVersions() throws Exception {
        SchemaStorage schemaStorage = buildSchemaStorage();
        try {
            byte[] value = putSchema(schemaStorage);

            CompletableFuture<List<CompletableFuture<StoredSchema>>> schemas = schemaStorage.getAll("key1");
            List<CompletableFuture<StoredSchema>> futures = schemas.get();
            Assert.assertEquals(futures.size(), 3);

            for (CompletableFuture<StoredSchema> future : futures) {
                StoredSchema result = future.get();
                Assert.assertEquals(new String(value), new String(result.data));
                long v = ((LongSchemaVersion) schemaStorage.versionFromBytes(result.version.bytes())).getVersion();
                Assert.assertTrue(v == 1 || v == 2 || v == 3);
            }
        } finally {
            schemaStorage.close();
        }
    }

    @Test
    public void testDeleteSchema() throws Exception {
        SchemaStorage schemaStorage = buildSchemaStorage();
        try {
            putSchema(schemaStorage);
            schemaStorage.delete("key1").get();
            CompletableFuture<List<CompletableFuture<StoredSchema>>> schemas = schemaStorage.getAll("key1");
            List<CompletableFuture<StoredSchema>> futures = schemas.get();
            Assert.assertEquals(futures.size(), 0);
        } finally {
            schemaStorage.close();
        }
    }

    static class GrpcSchemaStorageServer extends GrpcSchemaStorageServiceGrpc.GrpcSchemaStorageServiceImplBase {
        private final Map<String, AtomicInteger> versionMap = new ConcurrentHashMap<>();
        private final Map<String, Map<Long, byte[]>> schemaStorage = new ConcurrentHashMap<>();

        @Override
        public void get(GetVersionSchemaRequest request,
                        StreamObserver<VersionedSchemaResponse> responseObserver) {
            String key = request.getKey();
            byte[] versionBytes = request.getVersion().getSchemaVersion().toByteArray();
            long version = ByteBuffer.wrap(versionBytes).getLong();

            VersionedSchemaResponse ret = null;
            Map<Long, byte[]> versionMap = schemaStorage.get(key);
            if (versionMap != null && versionMap.containsKey(version)) {
                byte[] data = versionMap.get(version);
                ret = VersionedSchemaResponse.newBuilder()
                        .setData(ByteString.copyFrom(data))
                        .setVersion(GrpcSchemaVersion.newBuilder()
                                .setSchemaVersion(ByteString.copyFrom(versionBytes)).build())
                        .build();
            }
            responseObserver.onNext(ret);
            responseObserver.onCompleted();
        }

        @Override
        public void getAll(GetAllVersionSchemaRequest request, StreamObserver<GetAllVersionSchemaResponse> responseObserver) {
            String key = request.getKey();
            Map<Long, byte[]> versionMap = schemaStorage.get(key);
            List<VersionedSchemaResponse> rets = new ArrayList<>();
            if (versionMap != null && !versionMap.isEmpty()) {
                versionMap.forEach((version, bytes) -> {
                    byte[] versionBytes = new byte[8];
                    ByteBuffer.wrap(versionBytes).putLong(version);
                    VersionedSchemaResponse ret = VersionedSchemaResponse.newBuilder()
                            .setData(ByteString.copyFrom(bytes))
                            .setVersion(GrpcSchemaVersion.newBuilder()
                                    .setSchemaVersion(ByteString.copyFrom(versionBytes)).build())
                            .build();
                    rets.add(ret);
                });
            }

            GetAllVersionSchemaResponse allSchemas = GetAllVersionSchemaResponse
                    .newBuilder().addAllSchemas(rets).build();

            responseObserver.onNext(allSchemas);
            responseObserver.onCompleted();
        }

        @Override
        public void put(PutSchemaRequest request,
                        StreamObserver<GrpcSchemaVersion> responseObserver) {
            String key = request.getKey();
            byte[] data = request.getValue().toByteArray();
            byte[] hash = request.getHash().toByteArray();
            long version = versionMap.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
            Map<Long, byte[]> storage = schemaStorage.computeIfAbsent(key, k -> new HashMap<>());
            storage.put(version, data);
            byte[] versionBytes = new byte[8];
            ByteBuffer.wrap(versionBytes).putLong(version);
            responseObserver.onNext(GrpcSchemaVersion
                    .newBuilder().setSchemaVersion(ByteString.copyFrom(versionBytes)).build());
            responseObserver.onCompleted();
        }

        @Override
        public void delete(DeleteSchemaRequest request,
                           StreamObserver<GrpcSchemaVersion> responseObserver) {
            String key = request.getKey();
            Map<Long, byte[]> values = schemaStorage.remove(key);
            if (values.isEmpty()) {
                responseObserver.onNext(null);
                responseObserver.onCompleted();
                return;
            }

            long maxVersion = values.keySet().stream().max(Comparator.comparingLong(Long::longValue)).get();
            byte[] versionBytes = new byte[8];
            ByteBuffer.wrap(versionBytes).putLong(maxVersion);

            responseObserver.onNext(GrpcSchemaVersion.newBuilder()
                    .setSchemaVersion(ByteString.copyFrom(versionBytes)).build());
            responseObserver.onCompleted();
        }
    }
}
