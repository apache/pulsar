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

import org.apache.pulsar.common.protocol.PulsarDecoder;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandAck;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConnect;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandConnected;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandError;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandFlow;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandLookupTopicResponse;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandMessage;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandProducer;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandProducerSuccess;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSend;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSendError;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSendReceipt;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSubscribe;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandSuccess;
import org.apache.pulsar.common.api.proto.PulsarApi.CommandUnsubscribe;

import com.google.common.collect.Queues;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 */
public class ClientChannelHelper {
    private EmbeddedChannel channel;

    private Queue<Object> queue = Queues.newArrayDeque();

    public ClientChannelHelper() {
        int MaxMessageSize = 5 * 1024 * 1024;
        channel = new EmbeddedChannel(new LengthFieldBasedFrameDecoder(MaxMessageSize, 0, 4, 0, 4), decoder);
    }

    public Object getCommand(Object obj) {
        channel.writeInbound(obj);
        return queue.poll();
    }

    private PulsarDecoder decoder = new PulsarDecoder() {

        @Override
        protected void messageReceived() {
        }

        @Override
        protected void handleConnect(CommandConnect connect) {
            queue.offer(CommandConnect.newBuilder(connect).build());
        }

        @Override
        protected void handleConnected(CommandConnected connected) {
            queue.offer(CommandConnected.newBuilder(connected).build());
        }

        @Override
        protected void handleSubscribe(CommandSubscribe subscribe) {
            queue.offer(CommandSubscribe.newBuilder(subscribe).build());
        }

        @Override
        protected void handleProducer(CommandProducer producer) {
            queue.offer(CommandProducer.newBuilder(producer).build());
        }

        @Override
        protected void handleSend(CommandSend send, ByteBuf headersAndPayload) {
            queue.offer(CommandSend.newBuilder(send).build());
        }

        @Override
        protected void handleSendReceipt(CommandSendReceipt sendReceipt) {
            queue.offer(CommandSendReceipt.newBuilder(sendReceipt).build());
        }

        @Override
        protected void handleSendError(CommandSendError sendError) {
            queue.offer(CommandSendError.newBuilder(sendError).build());
        }

        @Override
        protected void handleMessage(CommandMessage cmdMessage, ByteBuf headersAndPayload) {
            queue.offer(CommandMessage.newBuilder(cmdMessage).build());
        }

        @Override
        protected void handleAck(CommandAck ack) {
            queue.offer(CommandAck.newBuilder(ack).build());
        }

        @Override
        protected void handleFlow(CommandFlow flow) {
            queue.offer(CommandFlow.newBuilder(flow).build());
        }

        @Override
        protected void handleUnsubscribe(CommandUnsubscribe unsubscribe) {
            queue.offer(CommandUnsubscribe.newBuilder(unsubscribe));
        }

        @Override
        protected void handleSuccess(CommandSuccess success) {
            queue.offer(CommandSuccess.newBuilder(success).build());
        }

        @Override
        protected void handleError(CommandError error) {
            queue.offer(CommandError.newBuilder(error).build());
        }

        @Override
        protected void handleCloseProducer(CommandCloseProducer closeProducer) {
            queue.offer(CommandCloseProducer.newBuilder(closeProducer).build());
        }

        @Override
        protected void handleCloseConsumer(CommandCloseConsumer closeConsumer) {
            queue.offer(CommandCloseConsumer.newBuilder(closeConsumer).build());
        }

        @Override
        protected void handleProducerSuccess(CommandProducerSuccess success) {
            queue.offer(CommandProducerSuccess.newBuilder(success).build());
        }

        @Override
        protected void handleLookupResponse(CommandLookupTopicResponse connection) {
            queue.offer(CommandLookupTopicResponse.newBuilder(connection).build());
        }
    };

}
