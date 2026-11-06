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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.lookup.GetTopicsResult;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiterImpl;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
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
        when(proxyService.getLookupRequestSemaphore()).thenReturn(new Semaphore(1));

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
        connectionFuture.complete(clientCnx);
        assertForwardedRequest(false);
        if (!alreadyFailed) {
            assertThat(channel.outboundMessages()).as("Responses before the broker completes").isEmpty();
            metadataFuture.completeExceptionally(error);
        }

        assertErrorResponse(expectedError, expectedMessage);
        verify(clientCnx).newLookup(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
        verify(connectionPool).releaseConnection(clientCnx);
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
    public void testPartitionMetadataReleasesConnectionOnResponseWriteFailure() {
        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        ChannelPromise promise = mock(ChannelPromise.class);
        when(context.voidPromise()).thenReturn(promise);
        doAnswer(invocation -> {
            ByteBuf response = invocation.getArgument(0);
            response.release();
            throw new IllegalStateException("Response write failed");
        }).when(context).writeAndFlush(any(ByteBuf.class), eq(promise));
        when(proxyConnection.ctx()).thenReturn(context);
        handler.handlePartitionMetadataResponse(newRequest());
        connectionFuture.complete(clientCnx);
        assertForwardedRequest(false);
        metadataFuture.complete(new LookupDataResult(4));

        verify(context).writeAndFlush(any(ByteBuf.class), eq(promise));
        verify(connectionPool).releaseConnection(clientCnx);
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
    }

    private CommandLookupTopic newLookupRequest() {
        return new CommandLookupTopic().setTopic(TOPIC).setRequestId(CLIENT_REQUEST_ID);
    }

    private CommandLookupTopicResponse readLookupErrorResponse(ServerError expectedError) {
        ByteBuf buffer = channel.readOutbound();
        assertThat(buffer).as("Lookup response").isNotNull();
        try {
            BaseCommand command = decodeCommand(buffer);
            assertThat(command.getType()).as("Response command type")
                    .isEqualTo(BaseCommand.Type.LOOKUP_RESPONSE);
            CommandLookupTopicResponse response = command.getLookupTopicResponse();
            assertThat(response.getRequestId()).as("Client request ID").isEqualTo(CLIENT_REQUEST_ID);
            assertThat(response.getResponse()).as("Lookup result")
                    .isEqualTo(CommandLookupTopicResponse.LookupType.Failed);
            assertThat(response.getError()).as("Lookup error code").isEqualTo(expectedError);
            assertThat(response.getMessage()).as("Lookup error message").isNotBlank();
            assertThat(channel.outboundMessages()).as("Additional responses").isEmpty();
            return new CommandLookupTopicResponse().copyFrom(response);
        } finally {
            buffer.release();
        }
    }

    private void assertGenericErrorResponse(ServerError expectedError) {
        ByteBuf buffer = channel.readOutbound();
        assertThat(buffer).as("Schema error response").isNotNull();
        try {
            BaseCommand command = decodeCommand(buffer);
            assertThat(command.getType()).as("Response command type").isEqualTo(BaseCommand.Type.ERROR);
            assertThat(command.getError().getRequestId()).as("Client request ID").isEqualTo(CLIENT_REQUEST_ID);
            assertThat(command.getError().getError()).as("Error code").isEqualTo(expectedError);
            assertThat(channel.outboundMessages()).as("Additional responses").isEmpty();
        } finally {
            buffer.release();
        }
        verify(connectionPool, never()).getConnection(any(InetSocketAddress.class));
        verifyNoBackendLookup();
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
