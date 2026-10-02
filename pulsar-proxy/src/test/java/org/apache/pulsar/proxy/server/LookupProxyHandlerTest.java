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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.BinaryProtoLookupService.LookupDataResult;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.common.api.proto.ServerError;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class LookupProxyHandlerTest {
    private static final long CLIENT_REQUEST_ID = 10L;
    private static final long BROKER_REQUEST_ID = 20L;
    private static final String TOPIC = "persistent://tenant/ns/topic";

    private CompletableFuture<LookupDataResult> metadataFuture;
    private CompletableFuture<ClientCnx> connectionFuture;
    private ClientCnx clientCnx;
    private ConnectionPool connectionPool;
    private EmbeddedChannel channel;
    private LookupProxyHandler handler;

    @BeforeMethod
    public void setup() {
        ProxyConfiguration configuration = new ProxyConfiguration();
        configuration.setBrokerServiceURL("pulsar://broker:6650");
        ProxyService proxyService = mock(ProxyService.class);
        when(proxyService.getConfiguration()).thenReturn(configuration);
        when(proxyService.getLookupRequestSemaphore()).thenReturn(new Semaphore(1));

        metadataFuture = new CompletableFuture<>();
        clientCnx = mock(ClientCnx.class);
        doAnswer(invocation -> {
            ByteBuf request = invocation.getArgument(0);
            try {
                BaseCommand command = decodeCommand(request);
                assertThat(command.getType()).as("Forwarded command type")
                        .isEqualTo(BaseCommand.Type.PARTITIONED_METADATA);
                assertThat(command.getPartitionMetadata().getRequestId()).as("Broker request ID")
                        .isEqualTo(BROKER_REQUEST_ID);
                assertThat(command.getPartitionMetadata().getTopic()).as("Forwarded topic").isEqualTo(TOPIC);
                assertThat(command.getPartitionMetadata().isMetadataAutoCreationEnabled())
                        .as("Metadata auto creation flag").isFalse();
                return metadataFuture;
            } finally {
                request.release();
            }
        }).when(clientCnx).newLookup(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
        connectionFuture = new CompletableFuture<>();
        connectionPool = mock(ConnectionPool.class);
        when(connectionPool.getConnection(any(InetSocketAddress.class))).thenReturn(connectionFuture);
        ProxyConnection proxyConnection = mock(ProxyConnection.class);
        when(proxyConnection.getConnectionPool()).thenReturn(connectionPool);
        when(proxyConnection.newRequestId()).thenReturn(BROKER_REQUEST_ID);

        channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        when(proxyConnection.ctx()).thenReturn(channel.pipeline().firstContext());
        handler = new LookupProxyHandler(proxyService, proxyConnection);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    @DataProvider
    public Object[][] partitionMetadataErrors() {
        return new Object[][] {
                {new PulsarClientException.AuthorizationException("Not authorized"), ServerError.AuthorizationError,
                        false},
                {new PulsarClientException.AuthorizationException("Not authorized"), ServerError.AuthorizationError,
                        true},
                {new ExecutionException(new PulsarClientException.TopicDoesNotExistException("Topic does not exist")),
                        ServerError.TopicNotFound, false},
                {new ExecutionException(new PulsarClientException.TopicDoesNotExistException("Topic does not exist")),
                        ServerError.TopicNotFound, true},
                {new PulsarClientException.TooManyRequestsException("Too many requests"), ServerError.TooManyRequests,
                        false},
                {new PulsarClientException.TooManyRequestsException("Too many requests"), ServerError.TooManyRequests,
                        true},
                {new PulsarClientException((String) null), ServerError.UnknownError, false},
                {new PulsarClientException((String) null), ServerError.UnknownError, true}
        };
    }

    @Test(dataProvider = "partitionMetadataErrors")
    public void testPartitionMetadataErrorResponse(Throwable error, ServerError expectedError,
                                                   boolean alreadyFailed) {
        if (alreadyFailed) {
            metadataFuture.completeExceptionally(error);
        }
        handler.handlePartitionMetadataResponse(newRequest());
        connectionFuture.complete(clientCnx);
        if (!alreadyFailed) {
            assertThat(channel.outboundMessages()).as("Responses before the broker completes").isEmpty();
            metadataFuture.completeExceptionally(error);
        }

        assertErrorResponse(expectedError, error.getMessage());
        verify(clientCnx).newLookup(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
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
        metadataFuture.complete(new LookupDataResult(partitions));

        CommandPartitionedTopicMetadataResponse response = readMetadataResponse();
        assertThat(response.getResponse()).as("Metadata lookup result")
                .isEqualTo(CommandPartitionedTopicMetadataResponse.LookupType.Success);
        assertThat(response.getPartitions()).as("Partition count").isEqualTo(partitions);
        assertThat(response.hasError()).as("Success response error").isFalse();
        verify(connectionPool).releaseConnection(clientCnx);
    }

    @Test
    public void testPartitionMetadataConnectionFailureResponse() {
        PulsarClientException.ConnectException error = new PulsarClientException.ConnectException("Connection failed");
        handler.handlePartitionMetadataResponse(newRequest());
        connectionFuture.completeExceptionally(error);

        // thenAccept propagates connection failures as a CompletionException.
        assertErrorResponse(ServerError.ServiceNotReady, error.toString());
        verify(clientCnx, never()).newLookup(any(ByteBuf.class), eq(BROKER_REQUEST_ID));
        verify(connectionPool, never()).releaseConnection(any(ClientCnx.class));
    }

    private CommandPartitionedTopicMetadata newRequest() {
        return new CommandPartitionedTopicMetadata()
                .setTopic(TOPIC)
                .setRequestId(CLIENT_REQUEST_ID)
                .setMetadataAutoCreationEnabled(false);
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

    private void assertErrorResponse(ServerError expectedError, String expectedMessage) {
        CommandPartitionedTopicMetadataResponse response = readMetadataResponse();
        assertThat(response.getResponse()).as("Metadata lookup result")
                .isEqualTo(CommandPartitionedTopicMetadataResponse.LookupType.Failed);
        assertThat(response.hasPartitions()).as("Partition count in an error response").isFalse();
        assertThat(response.hasError()).as("Error code presence").isTrue();
        assertThat(response.getError()).as("Broker error code").isEqualTo(expectedError);
        assertThat(response.hasMessage()).as("Error message presence").isEqualTo(expectedMessage != null);
        if (expectedMessage != null) {
            assertThat(response.getMessage()).as("Broker error message").isEqualTo(expectedMessage);
        }
    }
}
