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

import org.apache.pulsar.common.api.proto.PulsarApi;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 */
public interface MockBrokerServiceHooks {
    interface CommandConnectHook {
        void apply(ChannelHandlerContext ctx, PulsarApi.CommandConnect connect);
    }

    interface CommandPartitionLookupHook {
        void apply(ChannelHandlerContext ctx, PulsarApi.CommandPartitionedTopicMetadata connect);
    }

    interface CommandTopicLookupHook {
        void apply(ChannelHandlerContext ctx, PulsarApi.CommandLookupTopic connect);
    }

    interface CommandSubscribeHook {
        void apply(ChannelHandlerContext ctx, PulsarApi.CommandSubscribe subscribe);
    }

    interface CommandProducerHook {
        void apply(ChannelHandlerContext ctx, PulsarApi.CommandProducer producer);
    }

    interface CommandSendHook {
        void apply(ChannelHandlerContext ctx, PulsarApi.CommandSend send, ByteBuf headersAndPayload);
    }

    interface CommandAckHook {
        void apply(ChannelHandlerContext ctx, PulsarApi.CommandAck ack);
    }

    interface CommandFlowHook {
        void apply(ChannelHandlerContext ctx, PulsarApi.CommandFlow flow);
    }

    interface CommandUnsubscribeHook {
        void apply(ChannelHandlerContext ctx, PulsarApi.CommandUnsubscribe unsubscribe);
    }

    interface CommandCloseProducerHook {
        void apply(ChannelHandlerContext ctx, PulsarApi.CommandCloseProducer closeProducer);
    }

    interface CommandCloseConsumerHook {
        void apply(ChannelHandlerContext ctx, PulsarApi.CommandCloseConsumer closeConsumer);
    }
}
