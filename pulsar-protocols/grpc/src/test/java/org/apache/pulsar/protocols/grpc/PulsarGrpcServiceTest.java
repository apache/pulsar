package org.apache.pulsar.protocols.grpc;

import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.schema.DefaultSchemaRegistryService;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.protocols.grpc.api.*;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;

import static org.apache.pulsar.protocols.grpc.Constants.ERROR_CODE_METADATA_KEY;
import static org.apache.pulsar.protocols.grpc.Constants.PRODUCER_PARAMS_METADATA_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@Test
public class PulsarGrpcServiceTest {

    private static final Logger log = LoggerFactory.getLogger(PulsarGrpcServiceTest.class);

    private ServiceConfiguration svcConfig;
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
    private PulsarGrpc.PulsarStub stub;

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
        stub = PulsarGrpc.newStub(channel);
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
        Metadata headers = new Metadata();
        CommandProducer producerParams = Commands.newProducer(successTopicName,"prod-name", Collections.emptyMap());
        headers.put(PRODUCER_PARAMS_METADATA_KEY, producerParams.toByteArray());

        PulsarGrpc.PulsarStub producerStub = MetadataUtils.attachHeaders(stub, headers);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> request = producerStub.produce(observer);
        SendResult result = observer.takeOneMessage();

        assertTrue(result.hasProducerSuccess());

        // test SEND success
        PulsarApi.MessageMetadata messageMetadata = PulsarApi.MessageMetadata.newBuilder()
            .setPublishTime(System.currentTimeMillis())
            .setProducerName("prod-name")
            .setSequenceId(0)
            .build();
        ByteBuf data = Unpooled.buffer(1024);

        CommandSend clientCommand = Commands.newSend(1, 0, 1, ChecksumType.None, messageMetadata, data);

        request.onNext(clientCommand);
        SendResult sendReceipt = observer.takeOneMessage();
        assertTrue(sendReceipt.hasSendReceipt());

        request.onCompleted();
    }

    @Test(timeOut = 30000)
    public void testInvalidTopicOnProducer() throws Exception {
        String invalidTopicName = "xx/ass/aa/aaa";

        Metadata headers = new Metadata();
        CommandProducer producerParams = Commands.newProducer(invalidTopicName, "prod-name", Collections.emptyMap());
        headers.put(PRODUCER_PARAMS_METADATA_KEY, producerParams.toByteArray());

        PulsarGrpc.PulsarStub producerStub = MetadataUtils.attachHeaders(stub, headers);

        TestStreamObserver<SendResult> observer = new TestStreamObserver<>();

        StreamObserver<CommandSend> request = producerStub.produce(observer);
        Throwable t = observer.waitForError();
        assertErrorIsStatusExceptionWithServerError(t, Status.INVALID_ARGUMENT, ServerError.InvalidTopicName);

        request.onCompleted();
    }

    @Test(timeOut = 30000)
    public void testProducerOnNotOwnedTopic() throws Exception {
        // Force the case where the broker doesn't own any topic
        doReturn(false).when(namespaceService).isServiceUnitActive(any(TopicName.class));

        // test PRODUCER failure case
        Metadata headers = new Metadata();
        CommandProducer producerParams = Commands.newProducer(nonOwnedTopicName, "prod-name", Collections.emptyMap());
        headers.put(PRODUCER_PARAMS_METADATA_KEY, producerParams.toByteArray());

        PulsarGrpc.PulsarStub producerStub = MetadataUtils.attachHeaders(stub, headers);

        TestStreamObserver<SendResult> observer = new TestStreamObserver<>();

        StreamObserver<CommandSend> request = producerStub.produce(observer);
        Throwable t = observer.waitForError();
        assertErrorIsStatusExceptionWithServerError(t, Status.FAILED_PRECONDITION, ServerError.ServiceNotReady);

        assertFalse(pulsar.getBrokerService().getTopicReference(nonOwnedTopicName).isPresent());

        request.onCompleted();
    }

    @Test(timeOut = 30000)
    public void testUseSameProducerName() throws Exception {
        String producerName = "my-producer";
        // Create producer first time
        Metadata headers = new Metadata();
        CommandProducer producerParams = Commands.newProducer(successTopicName, producerName, Collections.emptyMap());
        headers.put(PRODUCER_PARAMS_METADATA_KEY, producerParams.toByteArray());
        PulsarGrpc.PulsarStub producerStub = MetadataUtils.attachHeaders(stub, headers);

        TestStreamObserver<SendResult> observer = TestStreamObserver.create();
        StreamObserver<CommandSend> produce = producerStub.produce(observer);

        assertTrue(observer.takeOneMessage().hasProducerSuccess());

        // Create producer second time
        TestStreamObserver<SendResult> observer2 = new TestStreamObserver<>();
        StreamObserver<CommandSend> produce2 = producerStub.produce(observer2);

        Throwable t = observer2.waitForError();
        assertErrorIsStatusExceptionWithServerError(t, Status.FAILED_PRECONDITION, ServerError.ProducerBusy);

        produce.onCompleted();
        produce2.onCompleted();
    }

    private static class TestStreamObserver<T> implements StreamObserver<T> {

        public static <T> TestStreamObserver<T> create() {
            return new TestStreamObserver<T>();
        }

        private LinkedBlockingQueue<T> queue = new LinkedBlockingQueue<>();
        private CompletableFuture<Throwable> error = new CompletableFuture<>();

        private TestStreamObserver() {
        }

        @Override
        public void onNext(T value) {
            queue.add(value);
        }

        @Override
        public void onError(Throwable t) {
            error.complete(t);
        }

        @Override
        public void onCompleted() {
        }

        public T takeOneMessage() throws InterruptedException {
            return queue.take();
        }

        public Throwable waitForError() throws ExecutionException, InterruptedException {
            return error.get();
        }
    }

    private static void assertErrorIsStatusExceptionWithServerError(Throwable actualException, Status expectedStatus, ServerError expectedCode) {
        Status actualStatus = Status.fromThrowable(actualException);
        assertEquals(actualStatus.getCode(), expectedStatus.getCode());

        Metadata actualMetadata = Status.trailersFromThrowable(actualException);
        assertNotNull(actualMetadata);
        assertEquals(actualMetadata.get(ERROR_CODE_METADATA_KEY), String.valueOf(expectedCode.getNumber()));
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
    
    @SuppressWarnings("unchecked")
    void setupMLAsyncCallbackMocks() {
        ledgerMock = mock(ManagedLedger.class);
        cursorMock = mock(ManagedCursor.class);
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

        doReturn(new ArrayList<Object>()).when(ledgerMock).getCursors();
        doReturn("mockCursor").when(cursorMock).getName();
        doReturn(true).when(cursorMock).isDurable();
        // doNothing().when(cursorMock).asyncClose(new CloseCallback() {
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                // return closeFuture.get();
                return closeFuture.complete(null);
            }
        })

                .when(cursorMock).asyncClose(new AsyncCallbacks.CloseCallback() {

            @Override
            public void closeComplete(Object ctx) {
                log.info("[{}] Successfully closed cursor ledger", "mockCursor");
                closeFuture.complete(null);
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                // isFenced.set(false);

                log.error("Error closing cursor for subscription", exception);
                closeFuture.completeExceptionally(new BrokerServiceException.PersistenceException(exception));
            }
        }, null);

        // call openLedgerComplete with ledgerMock on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.OpenLedgerCallback) invocationOnMock.getArguments()[2]).openLedgerComplete(ledgerMock, null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*success.*"), any(ManagedLedgerConfig.class),
                any(AsyncCallbacks.OpenLedgerCallback.class), any(Supplier.class), any());

        // call openLedgerFailed on ML factory asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.OpenLedgerCallback) invocationOnMock.getArguments()[2])
                        .openLedgerFailed(new ManagedLedgerException("Managed ledger failure"), null);
                return null;
            }
        }).when(mlFactoryMock).asyncOpen(matches(".*fail.*"), any(ManagedLedgerConfig.class),
                any(AsyncCallbacks.OpenLedgerCallback.class), any(Supplier.class), any());

        // call addComplete on ledger asyncAddEntry
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.AddEntryCallback) invocationOnMock.getArguments()[1]).addComplete(new PositionImpl(1, 1),
                        invocationOnMock.getArguments()[2]);
                return null;
            }
        }).when(ledgerMock).asyncAddEntry(any(ByteBuf.class), any(AsyncCallbacks.AddEntryCallback.class), any());

        // call openCursorComplete on cursor asyncOpen
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.OpenCursorCallback) invocationOnMock.getArguments()[2]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(PulsarApi.CommandSubscribe.InitialPosition.class), any(AsyncCallbacks.OpenCursorCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.OpenCursorCallback) invocationOnMock.getArguments()[3]).openCursorComplete(cursorMock, null);
                return null;
            }
        }).when(ledgerMock).asyncOpenCursor(matches(".*success.*"), any(PulsarApi.CommandSubscribe.InitialPosition.class), any(Map.class),
                any(AsyncCallbacks.OpenCursorCallback.class), any());

        // call deleteLedgerComplete on ledger asyncDelete
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.DeleteLedgerCallback) invocationOnMock.getArguments()[0]).deleteLedgerComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDelete(any(AsyncCallbacks.DeleteLedgerCallback.class), any());

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                ((AsyncCallbacks.DeleteCursorCallback) invocationOnMock.getArguments()[1]).deleteCursorComplete(null);
                return null;
            }
        }).when(ledgerMock).asyncDeleteCursor(matches(".*success.*"), any(AsyncCallbacks.DeleteCursorCallback.class), any());

        doAnswer((invokactionOnMock) -> {
            ((AsyncCallbacks.MarkDeleteCallback) invokactionOnMock.getArguments()[2])
                    .markDeleteComplete(invokactionOnMock.getArguments()[3]);
            return null;
        }).when(cursorMock).asyncMarkDelete(any(), any(), any(AsyncCallbacks.MarkDeleteCallback.class), any());
    }
}
