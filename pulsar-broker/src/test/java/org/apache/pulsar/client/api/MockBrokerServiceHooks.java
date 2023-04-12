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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandCloseConsumer;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.api.proto.CommandConnect;
import org.apache.pulsar.common.api.proto.CommandFlow;
import org.apache.pulsar.common.api.proto.CommandGetOrCreateSchema;
import org.apache.pulsar.common.api.proto.CommandLookupTopic;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.api.proto.CommandProducer;
import org.apache.pulsar.common.api.proto.CommandSend;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.CommandUnsubscribe;

public interface MockBrokerServiceHooks {
    interface CommandConnectHook {
        void apply(ChannelHandlerContext ctx, CommandConnect connect);
    }

    interface CommandPartitionLookupHook {
        void apply(ChannelHandlerContext ctx, CommandPartitionedTopicMetadata connect);
    }

    interface CommandTopicLookupHook {
        void apply(ChannelHandlerContext ctx, CommandLookupTopic connect);
    }

    interface CommandSubscribeHook {
        void apply(ChannelHandlerContext ctx, CommandSubscribe subscribe);
    }

    interface CommandProducerHook {
        void apply(ChannelHandlerContext ctx, CommandProducer producer);
    }

    interface CommandSendHook {
        void apply(ChannelHandlerContext ctx, CommandSend send, ByteBuf headersAndPayload);
    }

    interface CommandAckHook {
        void apply(ChannelHandlerContext ctx, CommandAck ack);
    }

    interface CommandFlowHook {
        void apply(ChannelHandlerContext ctx, CommandFlow flow);
    }

    interface CommandUnsubscribeHook {
        void apply(ChannelHandlerContext ctx, CommandUnsubscribe unsubscribe);
    }

    interface CommandCloseProducerHook {
        void apply(ChannelHandlerContext ctx, CommandCloseProducer closeProducer);
    }

    interface CommandCloseConsumerHook {
        void apply(ChannelHandlerContext ctx, CommandCloseConsumer closeConsumer);
    }

    interface CommandGetOrCreateSchemaHook {
        void apply(ChannelHandlerContext ctx, CommandGetOrCreateSchema closeConsumer);
    }
}
