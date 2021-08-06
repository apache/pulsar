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
package org.apache.pulsar.discovery.service;

import static org.apache.pulsar.broker.resources.MetadataStoreCacheLoader.LOADBALANCE_BROKERS_ROOT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.fail;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;
import org.apache.zookeeper.KeeperException.Code;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.awaitility.Awaitility;

public class DiscoveryServiceTest extends BaseDiscoveryTestSetup {

    private static final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/certificate/client.crt";
    private static final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/certificate/client.key";

    @BeforeMethod
    private void init() throws Exception {
        super.setup();
    }

    @AfterMethod(alwaysRun = true)
    private void clean() throws Exception {
        super.cleanup();
    }

    /**
     * Verifies: Discovery-service returns broker is round-robin manner
     *
     * @throws Exception
     */
    @Test
    public void testBrokerDiscoveryRoundRobin() throws Exception {
        addBrokerToZk(5);
        String prevUrl = null;
        BrokerDiscoveryProvider discoveryProvider = service.getDiscoveryProvider();
        for (int i = 0; i < 10; i++) {
            String current = discoveryProvider.nextBroker().getPulsarServiceUrl();
            assertNotEquals(prevUrl, current, "unexpected " + current + " vs " + prevUrl + ", available " + discoveryProvider.getAvailableBrokers());
            prevUrl = current;
        }
    }

    @Test
    public void testGetPartitionsMetadata() throws Exception {
        TopicName topic1 = TopicName.get("persistent://test/local/ns/my-topic-1");

        PartitionedTopicMetadata m = service.getDiscoveryProvider().getPartitionedTopicMetadata(service, topic1, "role", null)
                .get();
        assertEquals(m.partitions, 0);

        // Simulate ZK error
        simulateStoreError("/admin/partitioned-topics/test/local/ns/persistent/my-topic-2", Code.SESSIONEXPIRED);
        TopicName topic2 = TopicName.get("persistent://test/local/ns/my-topic-2");
        CompletableFuture<PartitionedTopicMetadata> future = service.getDiscoveryProvider()
                .getPartitionedTopicMetadata(service, topic2, "role", null);
        try {
            future.get();
            fail("Partition metadata lookup should have failed");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), MetadataStoreException.class);
        }
    }

    @Test
    public void testGetPartitionsMetadataForNonPersistentTopic() throws Exception {
        TopicName topic1 = TopicName.get("non-persistent://test/local/ns/my-topic-1");

        PartitionedTopicMetadata m = service.getDiscoveryProvider().getPartitionedTopicMetadata(service, topic1, "role", null)
                .get();
        assertEquals(m.partitions, 0);

        // Simulate ZK error
        simulateStoreErrorForNonPersistentTopic("/admin/partitioned-topics/test/local/ns/non-persistent/my-topic-2", Code.SESSIONEXPIRED);
        TopicName topic2 = TopicName.get("non-persistent://test/local/ns/my-topic-2");
        CompletableFuture<PartitionedTopicMetadata> future = service.getDiscoveryProvider()
                .getPartitionedTopicMetadata(service, topic2, "role", null);
        try {
            future.get();
            fail("Partition metadata lookup should have failed");
        } catch (ExecutionException e) {
            assertEquals(e.getCause().getClass(), MetadataStoreException.class);
        }
    }

    /**
     * It verifies: client connects to Discovery-service and receives discovery response successfully.
     *
     * @throws Exception
     */
    @Test
    public void testClientServerConnection() throws Exception {
        addBrokerToZk(2);

        final CompletableFuture<BaseCommand> promise = new CompletableFuture<>();
        NioEventLoopGroup workerGroup = connectToService(service.getServiceUrl(), promise, false);
        assertEquals(promise.get(10, TimeUnit.SECONDS).getType(), BaseCommand.Type.CONNECTED);
        workerGroup.shutdownGracefully();
    }

    @Test
    public void testClientServerConnectionTls() throws Exception {
        addBrokerToZk(2);

        final CompletableFuture<BaseCommand> promise = new CompletableFuture<>();
        NioEventLoopGroup workerGroup = connectToService(service.getServiceUrlTls(), promise, true);
        assertEquals(promise.get(10, TimeUnit.SECONDS).getType(), BaseCommand.Type.CONNECTED);
        workerGroup.shutdownGracefully();
    }

    /**
     * creates ClientHandler channel to connect and communicate with server
     *
     * @param serviceUrl
     * @param promise
     * @param tls
     * @return
     * @throws URISyntaxException
     */
    public static NioEventLoopGroup connectToService(String serviceUrl,
                                                     CompletableFuture<BaseCommand> promise,
                                                     boolean tls)
            throws URISyntaxException {
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);

        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                if (tls) {
                    SslContextBuilder builder = SslContextBuilder.forClient();
                    builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                    X509Certificate[] certificates = SecurityUtility
                            .loadCertificatesFromPemFile(TLS_CLIENT_CERT_FILE_PATH);
                    PrivateKey privateKey = SecurityUtility.loadPrivateKeyFromPemFile(TLS_CLIENT_KEY_FILE_PATH);
                    builder.keyManager(privateKey, (X509Certificate[]) certificates);
                    SslContext sslCtx = builder.build();
                    ch.pipeline().addLast("tls", sslCtx.newHandler(ch.alloc()));
                }
                ch.pipeline().addLast(new ClientHandler(promise));
            }
        });
        URI uri = new URI(serviceUrl);
        InetSocketAddress serviceAddress = new InetSocketAddress(uri.getHost(), uri.getPort());
        b.connect(serviceAddress).addListener((ChannelFuture future) -> {
            if (!future.isSuccess()) {
                promise.completeExceptionally(future.cause());
            }
        });
        return workerGroup;
    }

    static class ClientHandler extends ChannelInboundHandlerAdapter {

        final CompletableFuture<BaseCommand> promise;

        public ClientHandler(CompletableFuture<BaseCommand> promise) {
            this.promise = promise;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            try {
                ByteBuf buffer = (ByteBuf) msg;
                buffer.readUnsignedInt(); // discard frame length
                int cmdSize = (int) buffer.readUnsignedInt();
                BaseCommand cmd = new BaseCommand();
                cmd.parseFrom(buffer, cmdSize);
                buffer.release();

                promise.complete(cmd);
            } catch (Exception e) {
                promise.completeExceptionally(e);
            }
            ctx.close();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            promise.completeExceptionally(cause);
            ctx.close();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            ctx.writeAndFlush(Commands.newConnect("", "", null));
        }

    }

    private void addBrokerToZk(int number) throws Exception {

        for (int i = 0; i < number; i++) {
            LoadReport report = new LoadReport(null, null, "pulsar://broker-:15000" + i, null);
            String reportData = ObjectMapperFactory.getThreadLocal().writeValueAsString(report);
            zkStore.put(LOADBALANCE_BROKERS_ROOT + "/" + "broker-" + i,
                    reportData.getBytes(StandardCharsets.UTF_8), Optional.of(-1L)).get();
        }

        Awaitility.await().until(()
                -> service.getDiscoveryProvider().getAvailableBrokers().size() == number);
    }

}
