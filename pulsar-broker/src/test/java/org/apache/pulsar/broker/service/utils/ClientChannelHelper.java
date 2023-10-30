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
package org.apache.pulsar.broker.service.utils;

import java.util.Queue;
import org.apache.pulsar.common.api.proto.CommandAddPartitionToTxnResponse;
import org.apache.pulsar.common.api.proto.CommandAuthChallenge;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnPartitionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnOnSubscriptionResponse;
import org.apache.pulsar.common.api.proto.CommandEndTxnResponse;
import org.apache.pulsar.common.api.proto.CommandPing;
import org.apache.pulsar.common.api.proto.CommandPong;
import org.apache.pulsar.common.protocol.PulsarDecoder;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.CommandConnect;
import org.apache.pulsar.common.api.proto.CommandConnected;
import org.apache.pulsar.common.api.proto.CommandError;
import org.apache.pulsar.common.api.proto.CommandFlow;
import org.apache.pulsar.common.api.proto.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.CommandMessage;
import org.apache.pulsar.common.api.proto.CommandProducer;
import org.apache.pulsar.common.api.proto.CommandProducerSuccess;
import org.apache.pulsar.common.api.proto.CommandSend;
import org.apache.pulsar.common.api.proto.CommandSendError;
import org.apache.pulsar.common.api.proto.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.CommandSuccess;
import org.apache.pulsar.common.api.proto.CommandUnsubscribe;

import com.google.common.collect.Queues;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class ClientChannelHelper {
    private final EmbeddedChannel channel;

    private final Queue<Object> queue = Queues.newArrayDeque();

    public ClientChannelHelper() {
        int MaxMessageSize = 5 * 1024 * 1024;
        channel = new EmbeddedChannel(new LengthFieldBasedFrameDecoder(MaxMessageSize, 0, 4, 0, 4), decoder);
    }

    public Object getCommand(Object obj) {
        channel.writeInbound(obj);
        return queue.poll();
    }

    private final PulsarDecoder decoder = new PulsarDecoder() {

        @Override
        protected void messageReceived() {
        }

        @Override
        protected void handleConnect(CommandConnect connect) {
            queue.offer(new CommandConnect().copyFrom(connect));
        }

        @Override
        protected void handleConnected(CommandConnected connected) {
            queue.offer(new CommandConnected().copyFrom(connected));
        }

        @Override
        protected void handleAuthChallenge(CommandAuthChallenge challenge) {
            queue.offer(new CommandAuthChallenge().copyFrom(challenge));
        }

        @Override
        protected void handleSubscribe(CommandSubscribe subscribe) {
            queue.offer(new CommandSubscribe().copyFrom(subscribe));
        }

        @Override
        protected void handleProducer(CommandProducer producer) {
            queue.offer(new CommandProducer().copyFrom(producer));
        }

        @Override
        protected void handleSend(CommandSend send, ByteBuf headersAndPayload) {
            queue.offer(new CommandSend().copyFrom(send));
        }

        @Override
        protected void handleSendReceipt(CommandSendReceipt sendReceipt) {
            queue.offer(new CommandSendReceipt().copyFrom(sendReceipt));
        }

        @Override
        protected void handleSendError(CommandSendError sendError) {
            queue.offer(new CommandSendError().copyFrom(sendError));
        }

        @Override
        protected void handleMessage(CommandMessage cmdMessage, ByteBuf headersAndPayload) {
            queue.offer(new CommandMessage().copyFrom(cmdMessage));
        }

        @Override
        protected void handleAck(CommandAck ack) {
            queue.offer(new CommandAck().copyFrom(ack));
        }

        @Override
        protected void handleFlow(CommandFlow flow) {
            queue.offer(new CommandFlow().copyFrom(flow));
        }

        @Override
        protected void handleUnsubscribe(CommandUnsubscribe unsubscribe) {
            queue.offer(new CommandUnsubscribe().copyFrom(unsubscribe));
        }

        @Override
        protected void handleSuccess(CommandSuccess success) {
            queue.offer(new CommandSuccess().copyFrom(success));
        }

        @Override
        protected void handleError(CommandError error) {
            queue.offer(new CommandError().copyFrom(error));
        }

        @Override
        protected void handleCloseProducer(CommandCloseProducer closeProducer) {
            queue.offer(new CommandCloseProducer().copyFrom(closeProducer));
        }

        @Override
        protected void handleCloseConsumer(CommandCloseConsumer closeConsumer) {
            queue.offer(new CommandCloseConsumer().copyFrom(closeConsumer));
        }

        @Override
        protected void handleProducerSuccess(CommandProducerSuccess success) {
            queue.offer(new CommandProducerSuccess().copyFrom(success));
        }

        @Override
        protected void handleLookupResponse(CommandLookupTopicResponse connection) {
            queue.offer(new CommandLookupTopicResponse().copyFrom(connection));
        }

        @Override
        protected void handleAddPartitionToTxnResponse(
                CommandAddPartitionToTxnResponse commandAddPartitionToTxnResponse) {
            queue.offer(new CommandAddPartitionToTxnResponse().copyFrom(commandAddPartitionToTxnResponse));
        }

        @Override
        protected void handleEndTxnResponse(CommandEndTxnResponse commandEndTxnResponse) {
            queue.offer(new CommandEndTxnResponse().copyFrom(commandEndTxnResponse));
        }

        @Override
        protected void handleEndTxnOnPartitionResponse(
                CommandEndTxnOnPartitionResponse commandEndTxnOnPartitionResponse) {
            queue.offer(new CommandEndTxnOnPartitionResponse().copyFrom(commandEndTxnOnPartitionResponse));
        }

        @Override
        protected void handleEndTxnOnSubscriptionResponse(
                CommandEndTxnOnSubscriptionResponse commandEndTxnOnSubscriptionResponse) {
            queue.offer(new CommandEndTxnOnSubscriptionResponse().copyFrom(commandEndTxnOnSubscriptionResponse));
        }

        @Override
        protected void handlePing(CommandPing ping) {
            queue.offer(new CommandPing().copyFrom(ping));
        }

        @Override
        protected void handlePong(CommandPong pong) {
            return;
        }
    };

}
