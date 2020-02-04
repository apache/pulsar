package org.apache.pulsar.protocols.grpc;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.testing.StreamRecorder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoop;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.*;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.NoOpShutdownService;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.cache.ConfigurationCacheService;
import org.apache.pulsar.broker.cache.LocalZooKeeperCacheService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.PulsarServerCnx;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.schema.DefaultSchemaRegistryService;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.protocols.grpc.PulsarApi.*;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.pulsar.protocols.grpc.Constants.PRODUCER_PARAMS_METADATA_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doReturn;
import static org.testng.Assert.assertTrue;

@Test
public class PulsarGrpcServiceTest {

    private ServiceConfiguration svcConfig;
    private PulsarServerCnx serverCnx;
    protected BrokerService brokerService;
    private ManagedLedgerFactory mlFactoryMock;
    private PulsarService pulsar;
    private ConfigurationCacheService configCacheService;
    protected NamespaceService namespaceService;

    protected final String successTopicName = "persistent://prop/use/ns-abc/successTopic";
    private final String failTopicName = "persistent://prop/use/ns-abc/failTopic";
    private final String nonOwnedTopicName = "persistent://prop/use/ns-abc/success-not-owned-topic";
    private final String encryptionRequiredTopicName = "persistent://prop/use/ns-abc/successEncryptionRequiredTopic";
    private final String successSubName = "successSub";
    private final String nonExistentTopicName = "persistent://nonexistent-prop/nonexistent-cluster/nonexistent-namespace/successNonExistentTopic";
    private final String topicWithNonLocalCluster = "persistent://prop/usw/ns-abc/successTopic";

    private ManagedLedger ledgerMock = mock(ManagedLedger.class);
    private ManagedCursor cursorMock = mock(ManagedCursor.class);

    private OrderedExecutor executor;

    private Server server;
    private PulsarGrpcServiceGrpc.PulsarGrpcServiceStub stub;

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).build();
        svcConfig = spy(new ServiceConfiguration());
        pulsar = spy(new PulsarService(svcConfig));
        pulsar.setShutdownService(new NoOpShutdownService());
        doReturn(new DefaultSchemaRegistryService()).when(pulsar).getSchemaRegistryService();

        svcConfig.setKeepAliveIntervalSeconds(inSec(1, TimeUnit.SECONDS));
        svcConfig.setBacklogQuotaCheckEnabled(false);
        doReturn(svcConfig).when(pulsar).getConfiguration();

        doReturn("use").when(svcConfig).getClusterName();

        mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();
        ZooKeeperCache cache = mock(ZooKeeperCache.class);
        doReturn(30).when(cache).getZkOperationTimeoutSeconds();
        doReturn(cache).when(pulsar).getLocalZkCache();

        ZooKeeper mockZk = createMockZooKeeper();
        doReturn(mockZk).when(pulsar).getZkClient();
        doReturn(createMockBookKeeper(mockZk, ForkJoinPool.commonPool()))
            .when(pulsar).getBookKeeperClient();

        configCacheService = mock(ConfigurationCacheService.class);
        ZooKeeperDataCache<Policies> zkDataCache = mock(ZooKeeperDataCache.class);
        doReturn(Optional.empty()).when(zkDataCache).get(any());
        doReturn(zkDataCache).when(configCacheService).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();

        LocalZooKeeperCacheService zkCache = mock(LocalZooKeeperCacheService.class);
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(zkDataCache).getAsync(any());
        doReturn(zkDataCache).when(zkCache).policiesCache();
        doReturn(configCacheService).when(pulsar).getConfigurationCache();
        doReturn(zkCache).when(pulsar).getLocalZkCacheService();

        brokerService = spy(new BrokerService(pulsar));
        doReturn(brokerService).when(pulsar).getBrokerService();
        doReturn(executor).when(pulsar).getOrderedExecutor();

        namespaceService = mock(NamespaceService.class);
        doReturn(namespaceService).when(pulsar).getNamespaceService();
        doReturn(true).when(namespaceService).isServiceUnitOwned(any());
        doReturn(true).when(namespaceService).isServiceUnitActive(any());

        setupMLAsyncCallbackMocks();

        String serverName = InProcessServerBuilder.generateName();

        server = InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(ServerInterceptors.intercept(
                new PulsarGrpcService(brokerService, new NioEventLoopGroup()),
                Collections.singletonList(new GrpcServerInterceptor())
            ))
            .build();

        server.start();

        ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        stub = PulsarGrpcServiceGrpc.newStub(channel);
    }

    @AfterMethod
    public void teardown() {
        server.shutdownNow();
    }

    private int inSec(int time, TimeUnit unit) {
        return (int) TimeUnit.SECONDS.convert(time, unit);
    }

    @Test(timeOut = 30000)
    public void testSendCommand() throws Exception {
        QueueStreamObserver<PulsarApi.BaseCommand> observer = QueueStreamObserver.create();

        Metadata headers = new Metadata();
        CommandProducer producerParams = CommandProducer.newBuilder()
            .setTopic(successTopicName)
            .setProducerName("prod-name")
            .build();
        headers.put(PRODUCER_PARAMS_METADATA_KEY, producerParams.toByteArray());

        PulsarGrpcServiceGrpc.PulsarGrpcServiceStub producerStub = MetadataUtils.attachHeaders(stub, headers);

        StreamObserver<BaseCommand> request = producerStub.produce(observer);
        BaseCommand producerSuccess = observer.take();

        assertTrue(producerSuccess.hasProducerSuccess());

        // test SEND success
        MessageMetadata messageMetadata = MessageMetadata.newBuilder()
            .setPublishTime(System.currentTimeMillis())
            .setProducerName("prod-name")
            .setSequenceId(0)
            .build();
        ByteBuf data = Unpooled.buffer(1024);

        CommandSend clientCommand = Commands.newSend(1, 0, 1, ChecksumType.None, messageMetadata, data);

        BaseCommand cmd = BaseCommand.newBuilder()
            .setSend(clientCommand)
            .setType(BaseCommand.Type.SEND)
            .build();

        request.onNext(cmd);
        BaseCommand sendReceipt = observer.take();
        assertTrue(sendReceipt.hasSendReceipt());
    }

    private static class QueueStreamObserver<T> implements StreamObserver<T> {

        public static <T> QueueStreamObserver<T> create() {
            return new QueueStreamObserver<T>();
        }

        private LinkedBlockingQueue<T> queue = new LinkedBlockingQueue<>();

        private QueueStreamObserver() {
        }

        @Override
        public void onNext(T value) {
            queue.add(value);
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }

        public T take() throws InterruptedException {
            return queue.take();
        }
    }



    public static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
            "".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList, CreateMode.PERSISTENT);

        zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList,
            CreateMode.PERSISTENT);
        return zk;
    }

    public static NonClosableMockBookKeeper createMockBookKeeper(ZooKeeper zookeeper,
                                                                 ExecutorService executor) throws Exception {
        return spy(new NonClosableMockBookKeeper(zookeeper, executor));
    }

    // Prevent the MockBookKeeper instance from being closed when the broker is restarted within a test
    public static class NonClosableMockBookKeeper extends PulsarMockBookKeeper {

        public NonClosableMockBookKeeper(ZooKeeper zk, ExecutorService executor) throws Exception {
            super(zk, executor);
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public void shutdown() {
            // no-op
        }

        public void reallyShutdown() {
            super.shutdown();
        }
    }


    private void setupMLAsyncCallbackMocks() {
        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();

        // call openLedgerComplete with ledgerMock on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((AsyncCallbacks.OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
            any(AsyncCallbacks.OpenLedgerCallback.class), any());

        // call openLedgerFailed on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                new Thread(() -> {
                    ((AsyncCallbacks.OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
                }).start();

                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class),
            any(AsyncCallbacks.OpenLedgerCallback.class), any());

        // call addComplete on ledger asyncAddEntry
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.AddEntryCallback) invocationOnMock.getArguments()[1]).addComplete(new PositionImpl(-1, -1),
                    invocationOnMock.getArguments()[2]);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AsyncCallbacks.AddEntryCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((AsyncCallbacks.OpenCursorCallback) invocationOnMock.getArguments()[2]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition.class), any(AsyncCallbacks.OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((AsyncCallbacks.OpenCursorCallback) invocationOnMock.getArguments()[3]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition.class), any(Map.class),
            any(AsyncCallbacks.OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((AsyncCallbacks.OpenCursorCallback) invocationOnMock.getArguments()[2])
                    .openCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*fail.*"), any(org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition.class), any(AsyncCallbacks.OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Thread.sleep(300);
                ((AsyncCallbacks.OpenCursorCallback) invocationOnMock.getArguments()[3])
                    .openCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*fail.*"), any(org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe.InitialPosition.class), any(Map.class),
            any(AsyncCallbacks.OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(AsyncCallbacks.DeleteCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.DeleteCursorCallback) invocationOnMock.getArguments()[1])
                    .deleteCursorFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(ledgerMock).asyncDeleteCursor(matches(".*fail.*"), any(AsyncCallbacks.DeleteCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.CloseCallback) invocationOnMock.getArguments()[0]).closeComplete(null);
                return null;
            }
        }).when(cursorMock).asyncClose(any(AsyncCallbacks.CloseCallback.class), any());

        doReturn(successSubName).when(cursorMock).getName();
    }
}
