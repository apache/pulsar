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
package org.apache.pulsar.client.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.pulsar.client.api.MockBrokerServiceHooks.CommandAckHook;
import org.apache.pulsar.client.api.MockBrokerServiceHooks.CommandCloseConsumerHook;
import org.apache.pulsar.client.api.MockBrokerServiceHooks.CommandCloseProducerHook;
import org.apache.pulsar.client.api.MockBrokerServiceHooks.CommandConnectHook;
import org.apache.pulsar.client.api.MockBrokerServiceHooks.CommandFlowHook;
import org.apache.pulsar.client.api.MockBrokerServiceHooks.CommandGetOrCreateSchemaHook;
import org.apache.pulsar.client.api.MockBrokerServiceHooks.CommandPartitionLookupHook;
import org.apache.pulsar.client.api.MockBrokerServiceHooks.CommandProducerHook;
import org.apache.pulsar.client.api.MockBrokerServiceHooks.CommandSendHook;
import org.apache.pulsar.client.api.MockBrokerServiceHooks.CommandSubscribeHook;
import org.apache.pulsar.client.api.MockBrokerServiceHooks.CommandTopicLookupHook;
import org.apache.pulsar.client.api.MockBrokerServiceHooks.CommandUnsubscribeHook;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.CommandConnect;
import org.apache.pulsar.common.api.proto.CommandFlow;
import org.apache.pulsar.common.api.proto.CommandGetOrCreateSchema;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse.LookupType;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.CommandPing;
import org.apache.pulsar.common.api.proto.CommandPong;
import org.apache.pulsar.common.api.proto.CommandProducer;
import org.apache.pulsar.common.api.proto.CommandSend;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.CommandUnsubscribe;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.PulsarDecoder;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MockBrokerService {
    private LookupData lookupData;

    private class genericResponseHandler extends AbstractHandler {
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final String lookupURI = "/lookup/v2/destination/persistent";
        private final String partitionMetadataURI = "/admin/persistent";
        private final PartitionedTopicMetadata singlePartitionedTopicMetadata = new PartitionedTopicMetadata(1);
        private final PartitionedTopicMetadata multiPartitionedTopicMetadata = new PartitionedTopicMetadata(4);
        private final PartitionedTopicMetadata nonPartitionedTopicMetadata = new PartitionedTopicMetadata();
        // regex to find a partitioned topic
        private final Pattern singlePartPattern = Pattern.compile(".*/part-.*");
        private final Pattern multiPartPattern = Pattern.compile(".*/multi-part-.*");

        @Override
        public void handle(String s, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                throws IOException, ServletException {
            String responseString;
            log.info("Received HTTP request {}", baseRequest.getRequestURI());
            if (baseRequest.getRequestURI().startsWith(lookupURI)) {
                response.setContentType("application/json;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                responseString = objectMapper.writeValueAsString(lookupData);
            } else if (baseRequest.getRequestURI().startsWith(partitionMetadataURI)) {
                response.setContentType("application/json;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                if (singlePartPattern.matcher(baseRequest.getRequestURI()).matches()) {
                    responseString = objectMapper.writeValueAsString(singlePartitionedTopicMetadata);
                } else if (multiPartPattern.matcher(baseRequest.getRequestURI()).matches()) {
                    responseString = objectMapper.writeValueAsString(multiPartitionedTopicMetadata);
                } else {
                    responseString = objectMapper.writeValueAsString(nonPartitionedTopicMetadata);
                }
            } else {
                response.setContentType("text/html;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                responseString = "URI NOT DEFINED";
            }
            baseRequest.setHandled(true);
            response.getWriter().println(responseString);
            log.info("Sent response: {}", responseString);
        }
    }

    private class MockServerCnx extends PulsarDecoder {
        // local state
        ChannelHandlerContext ctx;
        long producerId = 0;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            this.ctx = ctx;
        }

        @Override
        protected void messageReceived() {
        }

        @Override
        protected void handleConnect(CommandConnect connect) {
            if (handleConnect != null) {
                handleConnect.apply(ctx, connect);
                return;
            }
            // default
            ctx.writeAndFlush(Commands.newConnected(connect.getProtocolVersion()));
        }

        @Override
        protected void handlePartitionMetadataRequest(CommandPartitionedTopicMetadata request) {
            if (handlePartitionlookup != null) {
                handlePartitionlookup.apply(ctx, request);
                return;
            }
            // default
            ctx.writeAndFlush(Commands.newPartitionMetadataResponse(0, request.getRequestId()));
        }

        @Override
        protected void handleLookup(CommandLookupTopic lookup) {
            if (handleTopiclookup != null) {
                handleTopiclookup.apply(ctx, lookup);
                return;
            }
            // default
            ctx.writeAndFlush(Commands.newLookupResponse(getBrokerAddress(), null, true,
                    LookupType.Connect, lookup.getRequestId(), false));
        }

        @Override
        protected void handleSubscribe(CommandSubscribe subscribe) {
            if (handleSubscribe != null) {
                handleSubscribe.apply(ctx, subscribe);
                return;
            }
            // default
            ctx.writeAndFlush(Commands.newSuccess(subscribe.getRequestId()));
        }

        @Override
        protected void handleProducer(CommandProducer producer) {
            producerId = producer.getProducerId();
            if (handleProducer != null) {
                handleProducer.apply(ctx, producer);
                return;
            }
            // default
            ctx.writeAndFlush(Commands.newProducerSuccess(producer.getRequestId(), "default-producer", SchemaVersion.Empty));
        }

        @Override
        protected void handleSend(CommandSend send, ByteBuf headersAndPayload) {
            if (handleSend != null) {
                handleSend.apply(ctx, send, headersAndPayload);
                return;
            }
            // default
            ctx.writeAndFlush(Commands.newSendReceipt(producerId, send.getSequenceId(), 0, 0, 0));
        }

        @Override
        protected void handleAck(CommandAck ack) {
            if (handleAck != null) {
                handleAck.apply(ctx, ack);
            }
            // default: do nothing
        }

        @Override
        protected void handleFlow(CommandFlow flow) {
            if (handleFlow != null) {
                handleFlow.apply(ctx, flow);
            }
            // default: do nothing
        }

        @Override
        protected void handleUnsubscribe(CommandUnsubscribe unsubscribe) {
            if (handleUnsubscribe != null) {
                handleUnsubscribe.apply(ctx, unsubscribe);
                return;
            }
            // default
            ctx.writeAndFlush(Commands.newSuccess(unsubscribe.getRequestId()));
        }

        @Override
        protected void handleCloseProducer(CommandCloseProducer closeProducer) {
            if (handleCloseProducer != null) {
                handleCloseProducer.apply(ctx, closeProducer);
                return;
            }
            // default
            ctx.writeAndFlush(Commands.newSuccess(closeProducer.getRequestId()));
        }

        @Override
        protected void handleCloseConsumer(CommandCloseConsumer closeConsumer) {
            if (handleCloseConsumer != null) {
                handleCloseConsumer.apply(ctx, closeConsumer);
                return;
            }
            // default
            ctx.writeAndFlush(Commands.newSuccess(closeConsumer.getRequestId()));
        }

        @Override
        protected void handleGetOrCreateSchema(CommandGetOrCreateSchema commandGetOrCreateSchema) {
            if (handleGetOrCreateSchema != null) {
                handleGetOrCreateSchema.apply(ctx, commandGetOrCreateSchema);
                return;
            }

            // default
            ctx.writeAndFlush(
                    Commands.newGetOrCreateSchemaResponse(commandGetOrCreateSchema.getRequestId(),
                            SchemaVersion.Empty));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            log.warn("Got exception", cause);
            ctx.close();
        }

        @Override
        final protected void handlePing(CommandPing ping) {
            // Immediately reply success to ping requests
            ctx.writeAndFlush(Commands.newPong());
        }

        @Override
        final protected void handlePong(CommandPong pong) {
        }
    }

    private final Server server;
    EventLoopGroup workerGroup;
    private Channel listenChannel;

    private CommandConnectHook handleConnect = null;
    private CommandTopicLookupHook handleTopiclookup = null;
    private CommandPartitionLookupHook handlePartitionlookup = null;
    private CommandSubscribeHook handleSubscribe = null;
    private CommandProducerHook handleProducer = null;
    private CommandSendHook handleSend = null;
    private CommandAckHook handleAck = null;
    private CommandFlowHook handleFlow = null;
    private CommandUnsubscribeHook handleUnsubscribe = null;
    private CommandCloseProducerHook handleCloseProducer = null;
    private CommandCloseConsumerHook handleCloseConsumer = null;
    private CommandGetOrCreateSchemaHook handleGetOrCreateSchema = null;

    public MockBrokerService() {
        server = new Server(0);
        server.setHandler(new genericResponseHandler());
    }

    public void start() {
        try {
            server.start();
            log.info("Started web service on {}", getHttpAddress());

            startMockBrokerService();
            log.info("Started mock Pulsar service on {}", getBrokerAddress());

            lookupData = new LookupData(getBrokerAddress(), null,
                    getHttpAddress(), null);
        } catch (Exception e) {
            log.error("Error starting mock service", e);
        }
    }

    public void stop() {
        try {
            server.stop();
            workerGroup.shutdownGracefully();
        } catch (Exception e) {
            log.error("Error stopping mock service", e);
        }
    }

    public void startMockBrokerService() throws Exception {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("mock-pulsar-%s").build();
        final int numThreads = 2;

        final int MaxMessageSize = 5 * 1024 * 1024;

        try {
            workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, false, threadFactory);

            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(workerGroup, workerGroup);
            bootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
            bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(MaxMessageSize, 0, 4, 0, 4));
                    ch.pipeline().addLast("handler", new MockServerCnx());
                }
            });
            // Bind and start to accept incoming connections.
            listenChannel = bootstrap.bind(0).sync().channel();
        } catch (Exception e) {
            throw e;
        }
    }

    public void setHandleConnect(CommandConnectHook hook) {
        handleConnect = hook;
    }

    public void resetHandleConnect() {
        handleConnect = null;
    }

    public void setHandlePartitionLookup(CommandPartitionLookupHook hook) {
        handlePartitionlookup = hook;
    }

    public void resetHandlePartitionLookup() {
        handlePartitionlookup = null;
    }

    public void setHandleLookup(CommandTopicLookupHook hook) {
        handleTopiclookup = hook;
    }

    public void resetHandleLookup() {
        handleTopiclookup = null;
    }

    public void setHandleSubscribe(CommandSubscribeHook hook) {
        handleSubscribe = hook;
    }

    public void resetHandleSubscribe() {
        handleSubscribe = null;
    }

    public void setHandleProducer(CommandProducerHook hook) {
        handleProducer = hook;
    }

    public void resetHandleProducer() {
        handleProducer = null;
    }

    public void setHandleSend(CommandSendHook hook) {
        handleSend = hook;
    }

    public void resetHandleSend() {
        handleSend = null;
    }

    public void setHandleAck(CommandAckHook hook) {
        handleAck = hook;
    }

    public void resetHandleAck() {
        handleAck = null;
    }

    public void setHandleFlow(CommandFlowHook hook) {
        handleFlow = hook;
    }

    public void resetHandleFlow() {
        handleFlow = null;
    }

    public void setHandleUnsubscribe(CommandUnsubscribeHook hook) {
        handleUnsubscribe = hook;
    }

    public void resetHandleUnsubscribe() {
        handleUnsubscribe = null;
    }

    public void setHandleCloseProducer(CommandCloseProducerHook hook) {
        handleCloseProducer = hook;
    }

    public void resetHandleCloseProducer() {
        handleCloseProducer = null;
    }

    public void setHandleCloseConsumer(CommandCloseConsumerHook hook) {
        handleCloseConsumer = hook;
    }

    public void setHandleGetOrCreateSchema(CommandGetOrCreateSchemaHook hook) {
        handleGetOrCreateSchema = hook;
    }

    public void resetHandleCloseConsumer() {
        handleCloseConsumer = null;
    }

    public String getHttpAddress() {
        return String.format("http://localhost:%d", server.getURI().getPort());
    }

    public String getBrokerAddress() {
        return String.format("pulsar://localhost:%d", ((InetSocketAddress) listenChannel.localAddress()).getPort());
    }

    private static final Logger log = LoggerFactory.getLogger(MockBrokerService.class);
}
