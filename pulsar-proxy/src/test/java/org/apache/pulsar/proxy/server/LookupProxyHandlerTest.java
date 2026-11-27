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
package org.apache.pulsar.proxy.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.EncoderException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.topiclistlimit.TopicListSizeResultCache;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.BinaryProtoLookupService.LookupDataResult;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandGetSchema;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespaceResponse;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.AsyncDualMemoryLimiterPermit;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.LimitType;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiterImpl;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.utils.TestLogAppender;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class LookupProxyHandlerTest {
    private static final long CLIENT_REQUEST_ID = 10L;
    private static final long BROKER_REQUEST_ID = 20L;
    private static final String TOPIC = "persistent://tenant/ns/topic";
    private static final String NAMESPACE = "tenant/ns";

    private ProxyConfiguration configuration;
    private ProxyService proxyService;
    private ProxyConnection proxyConnection;
    private Semaphore lookupRequestSemaphore;
    private CompletableFuture<LookupDataResult> metadataFuture;
    private CompletableFuture<ClientCnx> connectionFuture;
    private ClientCnx clientCnx;
    private ConnectionPool connectionPool;
    private EmbeddedChannel channel;
    private LookupProxyHandler handler;
    private ByteBuf forwardedRequest;
    private AsyncDualMemoryLimiterImpl topicListLimiter;

    @BeforeMethod
    public void setup() {
        configuration = new ProxyConfiguration();
        configuration.setBrokerServiceURL("pulsar://broker:6650");
        proxyService = mock(ProxyService.class);
        when(proxyService.getConfiguration()).thenReturn(configuration);
        lookupRequestSemaphore = new Semaphore(1);
        when(proxyService.getLookupRequestSemaphore()).thenReturn(lookupRequestSemaphore);

        metadataFuture = new CompletableFuture<>();
        clientCnx = mock(ClientCnx.class);
        doAnswer(invocation -> {
            ByteBuf request = invocation.getArgument(0);
            try {
                forwardedRequest = request.copy();
                return metadataFuture;
            } finally {
                request.release();
            }
        }).when(clientCnx).newLookup(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
        connectionFuture = new CompletableFuture<>();
        connectionPool = mock(ConnectionPool.class);
        when(connectionPool.getConnection(any(InetSocketAddress.class))).thenReturn(connectionFuture);
        proxyConnection = mock(ProxyConnection.class);
        when(proxyConnection.getConnectionPool()).thenReturn(connectionPool);
        when(proxyConnection.newRequestId()).thenReturn(BROKER_REQUEST_ID);

        channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        when(proxyConnection.ctx()).thenReturn(channel.pipeline().firstContext());
        handler = new LookupProxyHandler(proxyService, proxyConnection);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (topicListLimiter != null) {
            topicListLimiter.close();
            topicListLimiter = null;
        }
        if (forwardedRequest != null) {
            forwardedRequest.release();
            forwardedRequest = null;
        }
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    @DataProvider
    public Object[][] partitionMetadataErrors() {
        Object[][] errors = {
                {new PulsarClientException.AuthorizationException("Not authorized"), ServerError.AuthorizationError,
                        "Not authorized"},
                {new CompletionException(new PulsarClientException.AuthenticationException("Authentication failed")),
                        ServerError.AuthenticationError, "Authentication failed"},
                {new ExecutionException(new PulsarClientException.TopicDoesNotExistException("Topic does not exist")),
                        ServerError.TopicNotFound, "Topic does not exist"},
                {new PulsarClientException.TooManyRequestsException("Too many requests"), ServerError.TooManyRequests,
                        "Too many requests"},
                {new EncoderException("Encoding failed"), ServerError.UnknownError, "Encoding failed"},
                {new CompletionException(new EncoderException("Encoding failed")),
                        ServerError.UnknownError, "Encoding failed"},
                {new CompletionException(new IllegalStateException("Unexpected broker failure")),
                        ServerError.UnknownError, "Unexpected broker failure"},
                {new PulsarClientException((String) null), ServerError.UnknownError, null},
                {new PulsarClientException.TimeoutException("Lookup timed out"), ServerError.ServiceNotReady,
                        "Lookup timed out"},
                {new PulsarClientException.ConnectException("Disconnected"), ServerError.ServiceNotReady,
                        "Disconnected"},
                {new PulsarClientException.AlreadyClosedException("Connection closed"), ServerError.ServiceNotReady,
                        "Connection closed"},
                {new PulsarClientException.NotConnectedException("Not connected"), ServerError.ServiceNotReady,
                        "Not connected"},
                {new TimeoutException("Lookup timed out"), ServerError.ServiceNotReady, "Lookup timed out"},
                {new ClosedChannelException(), ServerError.ServiceNotReady, null}
        };
        List<Object[]> cases = new ArrayList<>();
        for (Object[] error : errors) {
            for (boolean alreadyFailed : new boolean[] {false, true}) {
                cases.add(new Object[] {error[0], error[1], error[2], alreadyFailed});
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "partitionMetadataErrors")
    public void testPartitionMetadataErrorResponse(Throwable error, ServerError expectedError, String expectedMessage,
                                                   boolean alreadyFailed) {
        if (alreadyFailed) {
            metadataFuture.completeExceptionally(error);
        }
        handler.handlePartitionMetadataResponse(newRequest());
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while connecting").isZero();
        connectionFuture.complete(clientCnx);
        assertForwardedRequest(false);
        if (!alreadyFailed) {
            assertThat(channel.outboundMessages()).as("Responses before the broker completes").isEmpty();
            assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while awaiting metadata").isZero();
            metadataFuture.completeExceptionally(error);
        }

        assertErrorResponse(expectedError, expectedMessage);
        verify(clientCnx).newLookup(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
        verify(connectionPool).releaseConnection(clientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after metadata failure").isOne();
    }

    @DataProvider
    public Object[][] metadataAutoCreationFlags() {
        return new Object[][] {
                {false, false}, {false, true}, {true, false}, {true, true}, {null, false}, {null, true}
        };
    }

    @Test(dataProvider = "metadataAutoCreationFlags")
    public void testPartitionMetadataPreservesAutoCreationFlag(Boolean autoCreationEnabled, boolean alreadyConnected) {
        CommandPartitionedTopicMetadata request = new CommandPartitionedTopicMetadata()
                .setTopic(TOPIC).setRequestId(CLIENT_REQUEST_ID);
        if (autoCreationEnabled != null) {
            request.setMetadataAutoCreationEnabled(autoCreationEnabled);
        }
        if (alreadyConnected) {
            connectionFuture.complete(clientCnx);
        }
        handler.handlePartitionMetadataResponse(request);
        // The decoder clears its reusable command as soon as the handler returns.
        request.clear();
        if (!alreadyConnected) {
            connectionFuture.complete(clientCnx);
        }
        assertForwardedRequest(autoCreationEnabled == null || autoCreationEnabled);
        metadataFuture.complete(new LookupDataResult(4));

        assertThat(readMetadataResponse().getPartitions()).as("Partition count").isEqualTo(4);
        verify(connectionPool).releaseConnection(clientCnx);
    }

    @DataProvider
    public Object[][] partitionCounts() {
        return new Object[][] {{0}, {4}};
    }

    @Test(dataProvider = "partitionCounts")
    public void testPartitionMetadataSuccessResponse(int partitions) {
        handler.handlePartitionMetadataResponse(newRequest());
        connectionFuture.complete(clientCnx);
        assertForwardedRequest(false);
        metadataFuture.complete(new LookupDataResult(partitions));

        CommandPartitionedTopicMetadataResponse response = readMetadataResponse();
        assertThat(response.getResponse()).as("Metadata lookup result")
                .isEqualTo(CommandPartitionedTopicMetadataResponse.LookupType.Success);
        assertThat(response.getPartitions()).as("Partition count").isEqualTo(partitions);
        assertThat(response.hasError()).as("Success response error").isFalse();
        verify(connectionPool).releaseConnection(clientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after metadata response").isOne();
    }

    @DataProvider
    public Object[][] connectionErrors() {
        return new Object[][] {
                {new PulsarClientException("Connection already closed"),
                        ServerError.ServiceNotReady, "Connection already closed"},
                {new ChannelException("Connection failed"), ServerError.ServiceNotReady, "Connection failed"},
                {new PulsarClientException.ConnectException("Connection failed"),
                        ServerError.ServiceNotReady, "Connection failed"},
                {new CompletionException(new PulsarClientException.AuthenticationException("Authentication failed")),
                        ServerError.AuthenticationError, "Authentication failed"},
                {new ExecutionException(new CompletionException(
                        new PulsarClientException.AuthorizationException("Not authorized"))),
                        ServerError.AuthorizationError, "Not authorized"},
                {new PulsarClientException.TimeoutException("Connection timed out"),
                        ServerError.ServiceNotReady, "Connection timed out"},
                {new PulsarClientException.AlreadyClosedException("Connection closed"),
                        ServerError.ServiceNotReady, "Connection closed"},
                {new PulsarClientException.NotConnectedException("Not connected"), ServerError.ServiceNotReady,
                        "Not connected"},
                {new TimeoutException("Connection timed out"), ServerError.ServiceNotReady, "Connection timed out"},
                {new ClosedChannelException(), ServerError.ServiceNotReady, null}
        };
    }

    @Test(dataProvider = "connectionErrors")
    public void testPartitionMetadataConnectionFailureResponse(Throwable error, ServerError expectedError,
                                                               String expectedMessage) {
        handler.handlePartitionMetadataResponse(newRequest());
        connectionFuture.completeExceptionally(error);

        assertErrorResponse(expectedError, expectedMessage);
        verifyNoBackendLookup();
        verify(connectionPool, never()).releaseConnection(any(ClientCnx.class));
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after connection failure").isOne();
    }

    @Test
    public void testPartitionMetadataDiscoveryFailureResponse() throws Exception {
        BrokerDiscoveryProvider discoveryProvider = mock(BrokerDiscoveryProvider.class);
        when(discoveryProvider.nextBroker()).thenThrow(new PulsarServerException("No active broker is available"));
        configuration.setBrokerServiceURL(null);
        when(proxyService.getDiscoveryProvider()).thenReturn(discoveryProvider);
        handler = new LookupProxyHandler(proxyService, proxyConnection);

        handler.handlePartitionMetadataResponse(newRequest());

        assertErrorResponse(ServerError.ServiceNotReady, "No active broker is available");
        verify(connectionPool, never()).getConnection(any(InetSocketAddress.class));
        verifyNoBackendLookup();
    }

    @DataProvider
    public Object[][] missingBrokerServiceUrls() {
        return new Object[][] {{false, null}, {true, null}, {false, ""}, {true, ""}};
    }

    @Test(dataProvider = "missingBrokerServiceUrls")
    public void testPartitionMetadataMissingBrokerServiceUrl(boolean useTls, String serviceUrl) throws Exception {
        BrokerDiscoveryProvider discoveryProvider = mock(BrokerDiscoveryProvider.class);
        LoadManagerReport broker = mock(LoadManagerReport.class);
        when(broker.getPulsarServiceUrl()).thenReturn(useTls ? "pulsar://broker:6650" : serviceUrl);
        when(broker.getPulsarServiceUrlTls()).thenReturn(useTls ? serviceUrl : "pulsar+ssl://broker:6651");
        when(discoveryProvider.nextBroker()).thenReturn(broker);
        when(proxyService.getDiscoveryProvider()).thenReturn(discoveryProvider);
        configuration.setBrokerServiceURL(null);
        configuration.setTlsEnabledWithBroker(useTls);
        handler = new LookupProxyHandler(proxyService, proxyConnection);

        handler.handlePartitionMetadataResponse(newRequest());

        CommandPartitionedTopicMetadataResponse response = assertErrorResponse(ServerError.ServiceNotReady);
        assertThat(response.getMessage()).as("Missing broker service URL error").isNotBlank();
        verify(connectionPool, never()).getConnection(any(InetSocketAddress.class));
        verifyNoBackendLookup();
    }

    @DataProvider
    public Object[][] invalidBrokerServiceUrls() {
        return new Object[][] {
                {"pulsar://bad host:6650"}, {"pulsar:///broker:6650"}, {"pulsar://broker"},
                {"pulsar://broker:0"}, {"pulsar://broker:65536"}
        };
    }

    @Test(dataProvider = "invalidBrokerServiceUrls")
    public void testPartitionMetadataInvalidBrokerServiceUrl(String serviceUrl) {
        configuration.setBrokerServiceURL(serviceUrl);
        handler = new LookupProxyHandler(proxyService, proxyConnection);

        handler.handlePartitionMetadataResponse(newRequest());

        CommandPartitionedTopicMetadataResponse response = assertErrorResponse(ServerError.MetadataError);
        assertThat(response.getMessage()).as("Invalid broker service URL error").isNotBlank();
        verify(connectionPool, never()).getConnection(any(InetSocketAddress.class));
        verifyNoBackendLookup();
    }

    @Test
    public void testPartitionMetadataReleasesOriginalConnectionPoolAfterDisconnect() {
        handler.handlePartitionMetadataResponse(newRequest());
        connectionFuture.complete(clientCnx);
        assertForwardedRequest(false);
        // ProxyConnection clears its pool when the client disconnects.
        when(proxyConnection.getConnectionPool()).thenReturn(null);
        metadataFuture.complete(new LookupDataResult(4));

        assertThat(readMetadataResponse().getPartitions()).as("Partition count").isEqualTo(4);
        verify(connectionPool).releaseConnection(clientCnx);
    }

    @Test
    public void testPartitionMetadataReleasesConnectionOnSynchronousLookupFailure() {
        doAnswer(invocation -> {
            ByteBuf request = invocation.getArgument(0);
            request.release();
            throw new EncoderException("Encoding failed");
        }).when(clientCnx).newLookup(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
        handler.handlePartitionMetadataResponse(newRequest());
        connectionFuture.complete(clientCnx);

        assertErrorResponse(ServerError.UnknownError, "Encoding failed");
        verify(connectionPool).releaseConnection(clientCnx);
    }

    @Test
    public void testPartitionMetadataReleasesConnectionOnResponseWriteFailure() throws Exception {
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        ChannelPromise promise = mock(ChannelPromise.class);
        when(context.voidPromise()).thenReturn(promise);
        IllegalStateException writeFailure = new IllegalStateException("Response write failed");
        doAnswer(invocation -> {
            ByteBuf response = invocation.getArgument(0);
            response.release();
            throw writeFailure;
        }).when(context).writeAndFlush(any(ByteBuf.class), eq(promise));
        when(proxyConnection.ctx()).thenReturn(context);
        handler.handlePartitionMetadataResponse(newRequest());
        connectionFuture.complete(clientCnx);
        assertForwardedRequest(false);
        try (TestLogAppender appender = TestLogAppender.create(LookupProxyHandler.class)) {
            metadataFuture.complete(new LookupDataResult(4));
            await().untilAsserted(() -> assertThat(appender.getEvents())
                    .filteredOn(event -> event.getThrown() == writeFailure)
                    .as("Unexpected response write failures logged").hasSize(1));
        }

        verify(context).writeAndFlush(any(ByteBuf.class), eq(promise));
        verify(connectionPool).releaseConnection(clientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after response write failure").isOne();
    }

    @Test
    public void testGetTopicsOfNamespaceHoldsPermitUntilResponse() throws Exception {
        long heapMemoryLimit = 1024;
        long directMemoryLimit = 1024;
        configureTopicListLimits(heapMemoryLimit, directMemoryLimit).updateSize(1);
        CompletableFuture<GetTopicsResult> topicsFuture = stubGetTopicsOfNamespace();

        AsyncDualMemoryLimiterPermit blockingDirectPermit =
                topicListLimiter.acquire(directMemoryLimit, LimitType.DIRECT_MEMORY, () -> false).get(10, SECONDS);
        try {
            handler.handleGetTopicsOfNamespace(newGetTopicsOfNamespaceRequest());
            assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while connecting").isZero();
            ProxyConnection otherConnection = mock(ProxyConnection.class);
            when(otherConnection.ctx()).thenReturn(channel.pipeline().firstContext());
            when(otherConnection.getConnectionPool()).thenReturn(connectionPool);
            LookupProxyHandler otherHandler = new LookupProxyHandler(proxyService, otherConnection);
            otherHandler.handleLookup(newLookupRequest());
            readLookupErrorResponse(ServerError.TooManyRequests);
            otherHandler.handleGetTopicsOfNamespace(newGetTopicsOfNamespaceRequest());
            assertCommandError(ServerError.TooManyRequests);
            verify(otherConnection, never()).getConnectionPool();

            connectionFuture.complete(clientCnx);
            verify(clientCnx).newGetTopicsOfNamespace(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
            assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while awaiting the topic list").isZero();
            // An earlier heap waiter can proceed once the request queues its resize and releases its estimate.
            CompletableFuture<AsyncDualMemoryLimiterPermit> heapWaiter =
                    topicListLimiter.acquire(heapMemoryLimit, LimitType.HEAP_MEMORY, () -> false);
            topicsFuture.complete(new GetTopicsResult(List.of(TOPIC), null, false, true));
            AsyncDualMemoryLimiterPermit blockingHeapPermit = heapWaiter.get(10, SECONDS);
            try {
                assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while resizing heap memory").isZero();
            } finally {
                topicListLimiter.release(blockingHeapPermit);
            }
            await().untilAsserted(() ->
                    assertThat(topicListLimiter.getLimiter(LimitType.DIRECT_MEMORY).getQueueSize())
                            .as("Responses waiting for direct memory").isOne());
            assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while awaiting direct memory").isZero();
            assertThat(channel.outboundMessages()).as("Responses before direct memory is available").isEmpty();
        } finally {
            topicListLimiter.release(blockingDirectPermit);
        }

        await().untilAsserted(() ->
                assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after topic list response").isOne());
        channel.runPendingTasks();
        ByteBuf buffer = channel.readOutbound();
        assertThat(buffer).as("Topic list response").isNotNull();
        try {
            BaseCommand command = decodeCommand(buffer);
            assertThat(command.getType()).as("Response command type")
                    .isEqualTo(BaseCommand.Type.GET_TOPICS_OF_NAMESPACE_RESPONSE);
            CommandGetTopicsOfNamespaceResponse response = command.getGetTopicsOfNamespaceResponse();
            assertThat(response.getRequestId()).as("Client request ID").isEqualTo(CLIENT_REQUEST_ID);
            assertThat(response.getTopicsList()).as("Topics in the response").containsExactly(TOPIC);
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testGetTopicsOfNamespaceReleasesPermitOnConnectionFailure() {
        handler.handleGetTopicsOfNamespace(newGetTopicsOfNamespaceRequest());
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while connecting").isZero();
        connectionFuture.completeExceptionally(new PulsarClientException.ConnectException("Disconnected"));

        assertCommandError(ServerError.ServiceNotReady);
        verify(clientCnx, never()).newGetTopicsOfNamespace(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after connection failure").isOne();
    }

    @Test
    public void testGetTopicsOfNamespaceReleasesPermitOnBrokerFailure() {
        TopicListSizeResultCache.ResultHolder size = configureTopicListLimits(1024, 1024);
        CompletableFuture<GetTopicsResult> topicsFuture = stubGetTopicsOfNamespace();
        handler.handleGetTopicsOfNamespace(newGetTopicsOfNamespaceRequest());
        connectionFuture.complete(clientCnx);
        CompletableFuture<Long> waitingEstimate = size.getSizeAsync();
        assertThat(waitingEstimate).as("Size estimate while the first listing is pending").isNotDone();
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while awaiting the topic list").isZero();

        topicsFuture.completeExceptionally(new PulsarClientException.NotAllowedException("Listing rejected"));

        assertCommandError(ServerError.NotAllowedError);
        assertThat(waitingEstimate).as("Size estimate after the first listing failed").isCompletedWithValue(1024L);
        verify(connectionPool).releaseConnection(clientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after broker failure").isOne();
    }

    @Test
    public void testGetTopicsOfNamespaceReleasesPermitOnInitialHeapRejection() {
        TopicListSizeResultCache.ResultHolder size = configureTopicListLimits(1024, 1024);
        size.updateSize(1025);
        handler.handleGetTopicsOfNamespace(newGetTopicsOfNamespaceRequest());
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while connecting").isZero();
        connectionFuture.complete(clientCnx);

        assertThat(assertCommandError(ServerError.NotAllowedError)).as("Rejected memory limit").contains("heap");
        verify(clientCnx, never()).newGetTopicsOfNamespace(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
        verify(connectionPool).releaseConnection(clientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after initial heap rejection").isOne();
    }

    @Test
    public void testGetTopicsOfNamespaceReleasesPermitOnInitialHeapCancellation() throws Exception {
        long heapLimit = 1024;
        configureTopicListLimits(heapLimit, 1024);
        AsyncDualMemoryLimiterPermit blockingPermit =
                topicListLimiter.acquire(heapLimit, LimitType.HEAP_MEMORY, () -> false).get(10, SECONDS);
        try {
            handler.handleGetTopicsOfNamespace(newGetTopicsOfNamespaceRequest());
            connectionFuture.complete(clientCnx);
            assertThat(topicListLimiter.getLimiter(LimitType.HEAP_MEMORY).getQueueSize())
                    .as("Listings waiting for initial heap memory").isOne();
            assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while awaiting heap memory").isZero();
            channel.close().syncUninterruptibly();
        } finally {
            topicListLimiter.release(blockingPermit);
        }

        await().untilAsserted(() -> assertThat(lookupRequestSemaphore.availablePermits())
                .as("Permits after cancellation").isOne());
        verify(clientCnx, never()).newGetTopicsOfNamespace(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
        verify(connectionPool).releaseConnection(clientCnx);
        assertThat(channel.outboundMessages()).as("Responses for cancelled listing").isEmpty();
    }

    @DataProvider
    public Object[][] topicListResponseMemoryLimits() {
        return new Object[][] {{1L, 1024L, "heap"}, {1024L, 1L, "direct"}};
    }

    @Test(dataProvider = "topicListResponseMemoryLimits")
    public void testGetTopicsOfNamespaceReleasesPermitOnResponseMemoryRejection(
            long heapLimit, long directLimit, String rejectedLimit) {
        configureTopicListLimits(heapLimit, directLimit);
        CompletableFuture<GetTopicsResult> topicsFuture = stubGetTopicsOfNamespace();
        handler.handleGetTopicsOfNamespace(newGetTopicsOfNamespaceRequest());
        connectionFuture.complete(clientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while awaiting the topic list").isZero();
        verify(clientCnx).newGetTopicsOfNamespace(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
        topicsFuture.complete(new GetTopicsResult(List.of(TOPIC), null, false, true));

        assertThat(assertCommandError(ServerError.NotAllowedError)).as("Rejected memory limit").contains(rejectedLimit);
        verify(connectionPool).releaseConnection(clientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after response memory rejection").isOne();
    }

    private TopicListSizeResultCache.ResultHolder configureTopicListLimits(long heapLimit, long directLimit) {
        topicListLimiter = new AsyncDualMemoryLimiterImpl(heapLimit, 100, 30_000, directLimit, 100, 30_000);
        TopicListSizeResultCache sizeCache = new TopicListSizeResultCache(heapLimit,
                () -> topicListLimiter.getLimiter(LimitType.HEAP_MEMORY).getAcquiredPermits() == 0);
        when(proxyService.getMaxTopicListInFlightLimiter()).thenReturn(topicListLimiter);
        when(proxyService.getTopicListSizeResultCache()).thenReturn(sizeCache);
        handler = new LookupProxyHandler(proxyService, proxyConnection);
        return sizeCache.getTopicListSize(NAMESPACE, CommandGetTopicsOfNamespace.Mode.PERSISTENT);
    }

    private CompletableFuture<GetTopicsResult> stubGetTopicsOfNamespace() {
        CompletableFuture<GetTopicsResult> topicsFuture = new CompletableFuture<>();
        doAnswer(invocation -> {
            ByteBuf request = invocation.getArgument(0);
            request.release();
            return topicsFuture;
        }).when(clientCnx).newGetTopicsOfNamespace(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
        return topicsFuture;
    }

    private CommandGetTopicsOfNamespace newGetTopicsOfNamespaceRequest() {
        return new CommandGetTopicsOfNamespace().setNamespace(NAMESPACE).setRequestId(CLIENT_REQUEST_ID)
                .setMode(CommandGetTopicsOfNamespace.Mode.PERSISTENT);
    }

    @Test
    public void testGetTopicsOfNamespaceCopiesPropertiesBeforeConnecting() {
        topicListLimiter = new AsyncDualMemoryLimiterImpl(Long.MAX_VALUE, 100, 10_000,
                Long.MAX_VALUE, 100, 10_000);
        when(proxyService.getMaxTopicListInFlightLimiter()).thenReturn(topicListLimiter);
        when(proxyService.getTopicListSizeResultCache()).thenReturn(new TopicListSizeResultCache());
        handler = new LookupProxyHandler(proxyService, proxyConnection);
        CompletableFuture<GetTopicsResult> topicsFuture = new CompletableFuture<>();
        doAnswer(invocation -> {
            ByteBuf request = invocation.getArgument(0);
            try {
                forwardedRequest = request.copy();
                return topicsFuture;
            } finally {
                request.release();
            }
        }).when(clientCnx).newGetTopicsOfNamespace(any(ByteBuf.class), eq(BROKER_REQUEST_ID));

        // The decoder parses every frame into the same command and releases the frame after the handler returns.
        BaseCommand decoderCommand = new BaseCommand();
        ByteBuf firstFrame = newGetTopicsOfNamespaceFrame(Map.of("key1", "value1", "key2", "value2"));
        ByteBuf secondFrame = newGetTopicsOfNamespaceFrame(Map.of("other", "other-value"));
        try {
            parseFrame(decoderCommand, firstFrame);
            handler.handleGetTopicsOfNamespace(decoderCommand.getGetTopicsOfNamespace());
            firstFrame.release();
            firstFrame = null;
            parseFrame(decoderCommand, secondFrame);

            connectionFuture.complete(clientCnx);

            verify(clientCnx, timeout(10_000)).newGetTopicsOfNamespace(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
            BaseCommand command = decodeCommand(forwardedRequest);
            assertThat(command.getType()).as("Forwarded command type")
                    .isEqualTo(BaseCommand.Type.GET_TOPICS_OF_NAMESPACE);
            CommandGetTopicsOfNamespace forwarded = command.getGetTopicsOfNamespace();
            assertThat(forwarded.getRequestId()).as("Broker request ID").isEqualTo(BROKER_REQUEST_ID);
            assertThat(forwarded.getNamespace()).as("Forwarded namespace").isEqualTo(NAMESPACE);
            Map<String, String> forwardedProperties = new HashMap<>();
            for (KeyValue kv : forwarded.getPropertiesList()) {
                forwardedProperties.put(kv.getKey(), kv.getValue());
            }
            assertThat(forwardedProperties).as("Forwarded properties")
                    .containsExactlyInAnyOrderEntriesOf(Map.of("key1", "value1", "key2", "value2"));
        } finally {
            if (firstFrame != null) {
                firstFrame.release();
            }
            secondFrame.release();
        }
    }

    private static ByteBuf newGetTopicsOfNamespaceFrame(Map<String, String> properties) {
        return Commands.newGetTopicsOfNamespaceRequest(NAMESPACE, CLIENT_REQUEST_ID,
                CommandGetTopicsOfNamespace.Mode.PERSISTENT, null, null, properties);
    }

    private static void parseFrame(BaseCommand command, ByteBuf frame) {
        int frameSize = frame.readInt();
        assertThat(frameSize).as("Frame size").isEqualTo(frame.readableBytes());
        command.parseFrom(frame, frame.readInt());
    }

    private void verifyNoBackendLookup() {
        verify(clientCnx, never()).newLookup(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
    }

    @Test
    public void testSchemaDiscoveryFailurePreservesGenericErrorResponse() throws Exception {
        BrokerDiscoveryProvider discoveryProvider = mock(BrokerDiscoveryProvider.class);
        when(discoveryProvider.nextBroker()).thenThrow(new PulsarServerException("No active broker is available"));
        configuration.setBrokerServiceURL(null);
        when(proxyService.getDiscoveryProvider()).thenReturn(discoveryProvider);
        handler = new LookupProxyHandler(proxyService, proxyConnection);

        handler.handleGetSchema(new CommandGetSchema().setTopic(TOPIC).setRequestId(CLIENT_REQUEST_ID));

        assertGenericErrorResponse(ServerError.ServiceNotReady);
    }

    @Test
    public void testSchemaInvalidBrokerUrlPreservesGenericErrorResponse() {
        configuration.setBrokerServiceURL("pulsar://broker");
        handler = new LookupProxyHandler(proxyService, proxyConnection);

        handler.handleGetSchema(new CommandGetSchema().setTopic(TOPIC).setRequestId(CLIENT_REQUEST_ID));

        assertGenericErrorResponse(ServerError.MetadataError);
    }

    @Test
    public void testLookupDiscoveryFailureResponse() throws Exception {
        BrokerDiscoveryProvider discoveryProvider = mock(BrokerDiscoveryProvider.class);
        when(discoveryProvider.nextBroker()).thenThrow(new PulsarServerException("No active broker is available"));
        configuration.setBrokerServiceURL(null);
        when(proxyService.getDiscoveryProvider()).thenReturn(discoveryProvider);
        handler = new LookupProxyHandler(proxyService, proxyConnection);

        handler.handleLookup(newLookupRequest());

        assertThat(readLookupErrorResponse(ServerError.ServiceNotReady).getMessage())
                .as("Lookup error message").isEqualTo("No active broker is available");
        verify(connectionPool, never()).getConnection(any(InetSocketAddress.class));
    }

    @Test(dataProvider = "missingBrokerServiceUrls")
    public void testLookupMissingBrokerServiceUrl(boolean useTls, String serviceUrl) throws Exception {
        BrokerDiscoveryProvider discoveryProvider = mock(BrokerDiscoveryProvider.class);
        LoadManagerReport broker = mock(LoadManagerReport.class);
        when(broker.getPulsarServiceUrl()).thenReturn(useTls ? "pulsar://broker:6650" : serviceUrl);
        when(broker.getPulsarServiceUrlTls()).thenReturn(useTls ? serviceUrl : "pulsar+ssl://broker:6651");
        when(discoveryProvider.nextBroker()).thenReturn(broker);
        when(proxyService.getDiscoveryProvider()).thenReturn(discoveryProvider);
        configuration.setBrokerServiceURL(null);
        configuration.setTlsEnabledWithBroker(useTls);
        handler = new LookupProxyHandler(proxyService, proxyConnection);

        handler.handleLookup(newLookupRequest());

        readLookupErrorResponse(ServerError.ServiceNotReady);
        verify(connectionPool, never()).getConnection(any(InetSocketAddress.class));
    }

    @Test(dataProvider = "invalidBrokerServiceUrls")
    public void testLookupInvalidBrokerServiceUrl(String serviceUrl) {
        configuration.setBrokerServiceURL(serviceUrl);
        handler = new LookupProxyHandler(proxyService, proxyConnection);

        handler.handleLookup(newLookupRequest());

        readLookupErrorResponse(ServerError.MetadataError);
        verify(connectionPool, never()).getConnection(any(InetSocketAddress.class));
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after invalid broker URL").isOne();
    }

    @DataProvider
    public Object[][] connectionReleaseFailures() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "connectionReleaseFailures")
    public void testLookupHoldsPermitUntilSuccessfulResponse(boolean releaseFails) {
        String brokerUrl = "pulsar://owner:6650";
        if (releaseFails) {
            doThrow(new IllegalStateException("Release failed")).when(connectionPool).releaseConnection(clientCnx);
        }
        handler.handleLookup(newLookupRequest());
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while connecting").isZero();

        connectionFuture.complete(clientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while awaiting the lookup").isZero();
        metadataFuture.complete(new LookupDataResult(new CommandLookupTopicResponse()
                .setRequestId(BROKER_REQUEST_ID)
                .setResponse(CommandLookupTopicResponse.LookupType.Connect)
                .setBrokerServiceUrl(brokerUrl)));

        CommandLookupTopicResponse response = readLookupResponse();
        assertThat(response.getResponse()).isEqualTo(CommandLookupTopicResponse.LookupType.Connect);
        assertThat(response.getBrokerServiceUrl()).isEqualTo(brokerUrl);
        assertThat(response.isProxyThroughServiceUrl()).isTrue();
        verify(connectionPool).releaseConnection(clientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after lookup success").isOne();
    }

    @DataProvider
    public Object[][] connectionAuthenticationFailures() {
        var failure = new PulsarClientException.AuthenticationException("Handshake failed");
        return new Object[][] {{failure, failure.toString()}, {new CompletionException(failure), failure.toString()}};
    }

    @Test(dataProvider = "connectionAuthenticationFailures")
    public void testLookupConnectionAuthenticationFailure(Throwable failure, String expectedMessage) {
        handler.handleLookup(newLookupRequest());
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while connecting").isZero();
        connectionFuture.completeExceptionally(failure);

        assertThat(readLookupErrorResponse(ServerError.ServiceNotReady).getMessage()).isEqualTo(expectedMessage);
        verifyNoBackendLookup();
        verify(connectionPool, never()).releaseConnection(any(ClientCnx.class));
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after connection failure").isOne();
    }

    @Test
    public void testLookupBrokerAuthenticationFailure() {
        handler.handleLookup(newLookupRequest());
        connectionFuture.complete(clientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while awaiting the lookup").isZero();
        metadataFuture.completeExceptionally(new PulsarClientException.AuthenticationException("Lookup failed"));

        assertThat(readLookupErrorResponse(ServerError.AuthenticationError).getMessage()).isEqualTo("Lookup failed");
        verify(connectionPool).releaseConnection(clientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after lookup failure").isOne();
    }

    @Test
    public void testLookupHoldsPermitAcrossRedirect() {
        InetSocketAddress redirectedAddress = InetSocketAddress.createUnresolved("broker2", 6650);
        CompletableFuture<ClientCnx> redirectedConnectionFuture = new CompletableFuture<>();
        when(connectionPool.getConnection(redirectedAddress)).thenReturn(redirectedConnectionFuture);
        ClientCnx redirectedClientCnx = mock(ClientCnx.class);
        CompletableFuture<LookupDataResult> redirectedLookupFuture = new CompletableFuture<>();
        doAnswer(invocation -> {
            ByteBuf request = invocation.getArgument(0);
            request.release();
            return redirectedLookupFuture;
        }).when(redirectedClientCnx).newLookup(any(ByteBuf.class), eq(BROKER_REQUEST_ID));

        handler.handleLookup(newLookupRequest());
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while connecting").isZero();

        connectionFuture.complete(clientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while awaiting the first lookup").isZero();
        metadataFuture.complete(new LookupDataResult(new CommandLookupTopicResponse()
                .setRequestId(BROKER_REQUEST_ID)
                .setResponse(CommandLookupTopicResponse.LookupType.Redirect)
                .setBrokerServiceUrl("pulsar://broker2:6650")));
        verify(connectionPool).releaseConnection(clientCnx);
        verify(connectionPool).getConnection(redirectedAddress);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while connecting after redirect").isZero();

        redirectedConnectionFuture.complete(redirectedClientCnx);
        verify(redirectedClientCnx).newLookup(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits while awaiting redirected lookup").isZero();
        redirectedLookupFuture.completeExceptionally(new PulsarClientException.TimeoutException("Lookup timed out"));

        readLookupErrorResponse(ServerError.ServiceNotReady);
        verify(connectionPool).releaseConnection(redirectedClientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after redirected lookup failure").isOne();
    }

    @DataProvider
    public Object[][] invalidRedirectBrokerUrls() {
        return new Object[][] {{null, ServerError.ServiceNotReady}, {"pulsar://broker2", ServerError.MetadataError}};
    }

    @Test(dataProvider = "invalidRedirectBrokerUrls")
    public void testLookupRedirectToInvalidBrokerUrl(String redirectUrl, ServerError expectedError) {
        handler.handleLookup(newLookupRequest());
        connectionFuture.complete(clientCnx);
        CommandLookupTopicResponse redirect = new CommandLookupTopicResponse()
                .setRequestId(BROKER_REQUEST_ID)
                .setResponse(CommandLookupTopicResponse.LookupType.Redirect);
        if (redirectUrl != null) {
            redirect.setBrokerServiceUrl(redirectUrl);
        }
        metadataFuture.complete(new LookupDataResult(redirect));

        readLookupErrorResponse(expectedError);
        verify(connectionPool).releaseConnection(clientCnx);
        assertThat(lookupRequestSemaphore.availablePermits()).as("Permits after invalid redirect URL").isOne();
    }

    private CommandLookupTopic newLookupRequest() {
        return new CommandLookupTopic().setTopic(TOPIC).setRequestId(CLIENT_REQUEST_ID);
    }

    private CommandLookupTopicResponse readLookupErrorResponse(ServerError expectedError) {
        CommandLookupTopicResponse response = readLookupResponse();
        assertThat(response.getResponse()).as("Lookup result").isEqualTo(CommandLookupTopicResponse.LookupType.Failed);
        assertThat(response.getError()).as("Lookup error code").isEqualTo(expectedError);
        assertThat(response.getMessage()).as("Lookup error message").isNotBlank();
        return response;
    }

    private CommandLookupTopicResponse readLookupResponse() {
        ByteBuf buffer = channel.readOutbound();
        assertThat(buffer).as("Lookup response").isNotNull();
        try {
            BaseCommand command = decodeCommand(buffer);
            assertThat(command.getType()).as("Response command type")
                    .isEqualTo(BaseCommand.Type.LOOKUP_RESPONSE);
            CommandLookupTopicResponse response = command.getLookupTopicResponse();
            assertThat(response.getRequestId()).as("Client request ID").isEqualTo(CLIENT_REQUEST_ID);
            assertThat(channel.outboundMessages()).as("Additional responses").isEmpty();
            return new CommandLookupTopicResponse().copyFrom(response);
        } finally {
            buffer.release();
        }
    }

    private void assertGenericErrorResponse(ServerError expectedError) {
        assertCommandError(expectedError);
        verify(connectionPool, never()).getConnection(any(InetSocketAddress.class));
        verifyNoBackendLookup();
    }

    private String assertCommandError(ServerError expectedError) {
        ByteBuf buffer = channel.readOutbound();
        assertThat(buffer).as("Error response").isNotNull();
        try {
            BaseCommand command = decodeCommand(buffer);
            assertThat(command.getType()).as("Response command type").isEqualTo(BaseCommand.Type.ERROR);
            assertThat(command.getError().getRequestId()).as("Client request ID").isEqualTo(CLIENT_REQUEST_ID);
            assertThat(command.getError().getError()).as("Error code").isEqualTo(expectedError);
            assertThat(channel.outboundMessages()).as("Additional responses").isEmpty();
            return command.getError().getMessage();
        } finally {
            buffer.release();
        }
    }

    private CommandPartitionedTopicMetadata newRequest() {
        return new CommandPartitionedTopicMetadata()
                .setTopic(TOPIC)
                .setRequestId(CLIENT_REQUEST_ID)
                .setMetadataAutoCreationEnabled(false);
    }

    private void assertForwardedRequest(boolean autoCreationEnabled) {
        assertThat(forwardedRequest).as("Forwarded request").isNotNull();
        try {
            BaseCommand command = decodeCommand(forwardedRequest);
            assertThat(command.getType()).as("Forwarded command type")
                    .isEqualTo(BaseCommand.Type.PARTITIONED_METADATA);
            assertThat(command.getPartitionMetadata().getRequestId()).as("Broker request ID")
                    .isEqualTo(BROKER_REQUEST_ID);
            assertThat(command.getPartitionMetadata().getTopic()).as("Forwarded topic").isEqualTo(TOPIC);
            assertThat(command.getPartitionMetadata().isMetadataAutoCreationEnabled())
                    .as("Metadata auto creation flag").isEqualTo(autoCreationEnabled);
        } finally {
            forwardedRequest.release();
            forwardedRequest = null;
        }
    }

    private BaseCommand decodeCommand(ByteBuf buffer) {
        int frameSize = buffer.readInt();
        assertThat(frameSize).as("Frame size").isEqualTo(buffer.readableBytes());
        int commandSize = buffer.readInt();
        BaseCommand command = new BaseCommand();
        command.parseFrom(buffer, commandSize);
        assertThat(buffer.isReadable()).as("Bytes after the command").isFalse();
        return command;
    }

    private CommandPartitionedTopicMetadataResponse readMetadataResponse() {
        ByteBuf buffer = channel.readOutbound();
        assertThat(buffer).as("Partition metadata response").isNotNull();
        try {
            BaseCommand command = decodeCommand(buffer);
            assertThat(command.getType()).as("Response command type")
                    .isEqualTo(BaseCommand.Type.PARTITIONED_METADATA_RESPONSE);
            assertThat(command.hasPartitionMetadataResponse()).as("Partition metadata response payload").isTrue();
            assertThat(command.hasLookupTopicResponse()).as("Topic lookup response payload").isFalse();
            CommandPartitionedTopicMetadataResponse response = command.getPartitionMetadataResponse();
            assertThat(response.getRequestId()).as("Client request ID").isEqualTo(CLIENT_REQUEST_ID);
            assertThat(channel.outboundMessages()).as("Additional responses").isEmpty();
            return new CommandPartitionedTopicMetadataResponse().copyFrom(response);
        } finally {
            buffer.release();
        }
    }

    private CommandPartitionedTopicMetadataResponse assertErrorResponse(ServerError expectedError) {
        CommandPartitionedTopicMetadataResponse response = readMetadataResponse();
        assertThat(response.getResponse()).as("Metadata lookup result")
                .isEqualTo(CommandPartitionedTopicMetadataResponse.LookupType.Failed);
        assertThat(response.hasPartitions()).as("Partition count in an error response").isFalse();
        assertThat(response.hasError()).as("Error code presence").isTrue();
        assertThat(response.getError()).as("Broker error code").isEqualTo(expectedError);
        return response;
    }

    private void assertErrorResponse(ServerError expectedError, String expectedMessage) {
        CommandPartitionedTopicMetadataResponse response = assertErrorResponse(expectedError);
        assertThat(response.hasMessage()).as("Error message presence").isEqualTo(expectedMessage != null);
        if (expectedMessage != null) {
            assertThat(response.getMessage()).as("Broker error message").isEqualTo(expectedMessage);
        }
    }
}
