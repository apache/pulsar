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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.common.api.proto.BaseCommand.Type;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadFactory;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAckResponse;
import org.apache.pulsar.common.api.proto.CommandActiveConsumerChange;
import org.apache.pulsar.common.api.proto.CommandAddPartitionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandAddSubscriptionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.api.proto.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnPartitionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnSubscriptionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnResponse;
import org.apache.pulsar.common.api.proto.CommandError;
import org.apache.pulsar.common.api.proto.CommandGetLastMessageIdResponse;
import org.apache.pulsar.common.api.proto.CommandGetOrCreateSchemaResponse;
import org.apache.pulsar.common.api.proto.CommandGetSchemaResponse;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespaceResponse;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandMessage;
import org.apache.pulsar.common.api.proto.CommandNewTxnResponse;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadataResponse;
import org.apache.pulsar.common.api.proto.CommandPing;
import org.apache.pulsar.common.api.proto.CommandProducerSuccess;
import org.apache.pulsar.common.api.proto.CommandReachedEndOfTopic;
import org.apache.pulsar.common.api.proto.CommandSendError;
import org.apache.pulsar.common.api.proto.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.apache.pulsar.common.api.proto.CommandTcClientConnectResponse;
import org.apache.pulsar.common.api.proto.CommandTopicMigrated;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;
import org.apache.pulsar.common.api.proto.CommandWatchTopicUpdate;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

public class InjectedClientCnxClientBuilder {

    public static PulsarClientImpl create(final ClientBuilderImpl clientBuilder, final ClientCnxCustomizer customizer)
            throws Exception {
        ClientConfigurationData conf = clientBuilder.getClientConfigurationData();
        ThreadFactory threadFactory = new ExecutorProvider
                .ExtendedThreadFactory("pulsar-client-io", Thread.currentThread().isDaemon());
        EventLoopGroup eventLoopGroup =
                EventLoopUtil.newEventLoopGroup(conf.getNumIoThreads(), conf.isEnableBusyWait(), threadFactory);

        // Inject into ClientCnx.
        ConnectionPool pool = new ConnectionPool(conf, eventLoopGroup,
                () -> new InjectedClientCnx(conf, eventLoopGroup, customizer));

        return new PulsarClientImpl(conf, eventLoopGroup, pool);
    }

    public static abstract class ClientCnxCustomizer {

        protected final ConcurrentSkipListSet<BaseCommand.Type> injectedEvents;

        public ClientCnxCustomizer(Set<Type> injectedEvents) {
            this.injectedEvents = new ConcurrentSkipListSet(injectedEvents);
        }

        public ClientCnxCustomizer(Type...injectedEvents) {
            this.injectedEvents = new ConcurrentSkipListSet<>();
            for (Type type : injectedEvents) {
                this.injectedEvents.add(type);
            }
            if (this.injectedEvents.contains(Type.MESSAGE)) {
                throw new IllegalArgumentException("Not support for Type.MESSAGE now");
            }
        }

        public boolean shouldInjectEvent(Type commandType) {
            return injectedEvents.contains(commandType);
        }

        public abstract void handleCommand(Object command);
    }

    public static interface Executable {
        void execute() throws Exception;
    }

    public static class InjectedClientCnx extends ClientCnx {

        private final ClientCnxCustomizer customizer;

        public InjectedClientCnx(ClientConfigurationData conf, EventLoopGroup eventLoopGroup,
                                 ClientCnxCustomizer customizer) {
            super(conf, eventLoopGroup);
            this.customizer = customizer;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            super.channelRead(ctx, msg);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
        }

        @Override
        protected ByteBuf newConnectCommand() throws Exception {
            return super.newConnectCommand();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }

        @Override
        protected void handlePing(CommandPing ping) {
            if (customizer.shouldInjectEvent(Type.PING)) {
                customizer.handleCommand(ping);
                return;
            }
            super.handlePing(ping);
        }

        @Override
        protected void handleConnected(CommandConnected connected) {
            if (customizer.shouldInjectEvent(Type.CONNECTED)) {
                customizer.handleCommand(connected);
                return;
            }
            super.handleConnected(connected);
        }

        @Override
        protected void handleAuthChallenge(CommandAuthChallenge authChallenge) {
            if (customizer.shouldInjectEvent(Type.AUTH_CHALLENGE)) {
                customizer.handleCommand(authChallenge);
                return;
            }
            super.handleAuthChallenge(authChallenge);
        }

        @Override
        protected void handleSendReceipt(CommandSendReceipt sendReceipt) {
            if (customizer.shouldInjectEvent(Type.SEND_RECEIPT)) {
                customizer.handleCommand(sendReceipt);
                return;
            }
            super.handleSendReceipt(sendReceipt);
        }

        @Override
        protected void handleAckResponse(CommandAckResponse ackResponse) {
            if (customizer.shouldInjectEvent(Type.ACK_RESPONSE)) {
                customizer.handleCommand(ackResponse);
                return;
            }
            super.handleAckResponse(ackResponse);
        }


        @Override
        protected void handleMessage(CommandMessage cmdMessage, ByteBuf headersAndPayload) {
            // Not support this event now.
            super.handleMessage(cmdMessage, headersAndPayload);
        }

        @Override
        protected void handleActiveConsumerChange(CommandActiveConsumerChange change) {
            if (customizer.shouldInjectEvent(Type.ACTIVE_CONSUMER_CHANGE)) {
                customizer.handleCommand(change);
                return;
            }
            super.handleActiveConsumerChange(change);
        }

        @Override
        protected void handleSuccess(CommandSuccess success) {
            if (customizer.shouldInjectEvent(Type.SUCCESS)) {
                customizer.handleCommand(success);
                return;
            }
            super.handleSuccess(success);
        }

        @Override
        protected void handleGetLastMessageIdSuccess(CommandGetLastMessageIdResponse success) {
            if (customizer.shouldInjectEvent(Type.GET_LAST_MESSAGE_ID_RESPONSE)) {
                customizer.handleCommand(success);
                return;
            }
            super.handleGetLastMessageIdSuccess(success);
        }

        @Override
        protected void handleProducerSuccess(CommandProducerSuccess success) {
            if (customizer.shouldInjectEvent(Type.PRODUCER_SUCCESS)) {
                customizer.handleCommand(success);
                return;
            }
            super.handleProducerSuccess(success);
        }

        @Override
        protected void handleLookupResponse(CommandLookupTopicResponse lookupResult) {
            if (customizer.shouldInjectEvent(Type.LOOKUP_RESPONSE)) {
                customizer.handleCommand(lookupResult);
                return;
            }
            super.handleLookupResponse(lookupResult);
        }

        @Override
        protected void handlePartitionResponse(CommandPartitionedTopicMetadataResponse lookupResult) {
            if (customizer.shouldInjectEvent(Type.PARTITIONED_METADATA_RESPONSE)) {
                customizer.handleCommand(lookupResult);
                return;
            }
            super.handlePartitionResponse(lookupResult);
        }

        @Override
        protected void handleReachedEndOfTopic(CommandReachedEndOfTopic commandReachedEndOfTopic) {
            if (customizer.shouldInjectEvent(Type.REACHED_END_OF_TOPIC)) {
                customizer.handleCommand(commandReachedEndOfTopic);
                return;
            }
            super.handleReachedEndOfTopic(commandReachedEndOfTopic);
        }

        @Override
        protected void handleTopicMigrated(CommandTopicMigrated commandTopicMigrated) {
            if (customizer.shouldInjectEvent(Type.TOPIC_MIGRATED)) {
                customizer.handleCommand(commandTopicMigrated);
                return;
            }
            super.handleTopicMigrated(commandTopicMigrated);
        }

        @Override
        protected void handleSendError(CommandSendError sendError) {
            if (customizer.shouldInjectEvent(Type.SEND_ERROR)) {
                customizer.handleCommand(sendError);
                return;
            }
            super.handleSendError(sendError);
        }

        @Override
        protected void handleError(CommandError error) {
            if (customizer.shouldInjectEvent(Type.ERROR)) {
                customizer.handleCommand(error);
                return;
            }
            super.handleError(error);
        }

        @Override
        protected void handleCloseProducer(CommandCloseProducer closeProducer) {
            if (customizer.shouldInjectEvent(Type.CLOSE_PRODUCER)) {
                customizer.handleCommand(closeProducer);
                return;
            }
            super.handleCloseProducer(closeProducer);
        }

        @Override
        protected void handleCloseConsumer(CommandCloseConsumer closeConsumer) {
            if (customizer.shouldInjectEvent(Type.CLOSE_CONSUMER)) {
                customizer.handleCommand(closeConsumer);
                return;
            }
            super.handleCloseConsumer(closeConsumer);
        }

        @Override
        protected void handleGetTopicsOfNamespaceSuccess(CommandGetTopicsOfNamespaceResponse success) {
            if (customizer.shouldInjectEvent(Type.GET_TOPICS_OF_NAMESPACE_RESPONSE)) {
                customizer.handleCommand(success);
                return;
            }
            super.handleGetTopicsOfNamespaceSuccess(success);
        }

        @Override
        protected void handleGetSchemaResponse(CommandGetSchemaResponse commandGetSchemaResponse) {
            if (customizer.shouldInjectEvent(Type.GET_SCHEMA_RESPONSE)) {
                customizer.handleCommand(commandGetSchemaResponse);
                return;
            }
            super.handleGetSchemaResponse(commandGetSchemaResponse);
        }

        @Override
        protected void handleGetOrCreateSchemaResponse(CommandGetOrCreateSchemaResponse
                                                                       commandGetOrCreateSchemaResponse) {
            if (customizer.shouldInjectEvent(Type.GET_OR_CREATE_SCHEMA_RESPONSE)) {
                customizer.handleCommand(commandGetOrCreateSchemaResponse);
                return;
            }
            super.handleGetOrCreateSchemaResponse(commandGetOrCreateSchemaResponse);
        }

        @Override
        protected void handleNewTxnResponse(CommandNewTxnResponse command) {
            if (customizer.shouldInjectEvent(Type.NEW_TXN_RESPONSE)) {
                customizer.handleCommand(command);
                return;
            }
            super.handleNewTxnResponse(command);
        }

        @Override
        protected void handleAddPartitionToTxnResponse(CommandAddPartitionToTxnResponse command) {
            if (customizer.shouldInjectEvent(Type.ADD_PARTITION_TO_TXN_RESPONSE)) {
                customizer.handleCommand(command);
                return;
            }
            super.handleAddPartitionToTxnResponse(command);
        }

        @Override
        protected void handleAddSubscriptionToTxnResponse(CommandAddSubscriptionToTxnResponse command) {
            if (customizer.shouldInjectEvent(Type.ADD_SUBSCRIPTION_TO_TXN_RESPONSE)) {
                customizer.handleCommand(command);
                return;
            }
            super.handleAddSubscriptionToTxnResponse(command);
        }

        @Override
        protected void handleEndTxnOnPartitionResponse(CommandEndTxnOnPartitionResponse command) {
            if (customizer.shouldInjectEvent(Type.END_TXN_ON_PARTITION_RESPONSE)) {
                customizer.handleCommand(command);
                return;
            }
            super.handleEndTxnOnPartitionResponse(command);
        }

        @Override
        protected void handleEndTxnOnSubscriptionResponse(CommandEndTxnOnSubscriptionResponse command) {
            if (customizer.shouldInjectEvent(Type.END_TXN_ON_SUBSCRIPTION_RESPONSE)) {
                customizer.handleCommand(command);
                return;
            }
            super.handleEndTxnOnSubscriptionResponse(command);
        }

        @Override
        protected void handleEndTxnResponse(CommandEndTxnResponse command) {
            if (customizer.shouldInjectEvent(Type.END_TXN_RESPONSE)) {
                customizer.handleCommand(command);
                return;
            }
            super.handleEndTxnResponse(command);
        }

        @Override
        protected void handleTcClientConnectResponse(CommandTcClientConnectResponse response) {
            if (customizer.shouldInjectEvent(Type.TC_CLIENT_CONNECT_RESPONSE)) {
                customizer.handleCommand(response);
                return;
            }
            super.handleTcClientConnectResponse(response);
        }

        @Override
        protected void handleCommandWatchTopicListSuccess(CommandWatchTopicListSuccess commandWatchTopicListSuccess) {
            if (customizer.shouldInjectEvent(Type.WATCH_TOPIC_LIST_SUCCESS)) {
                customizer.handleCommand(commandWatchTopicListSuccess);
                return;
            }
            super.handleCommandWatchTopicListSuccess(commandWatchTopicListSuccess);
        }

        @Override
        protected void handleCommandWatchTopicUpdate(CommandWatchTopicUpdate commandWatchTopicUpdate) {
            if (customizer.shouldInjectEvent(Type.WATCH_TOPIC_UPDATE)) {
                customizer.handleCommand(commandWatchTopicUpdate);
                return;
            }
            super.handleCommandWatchTopicUpdate(commandWatchTopicUpdate);
        }
    }
}
