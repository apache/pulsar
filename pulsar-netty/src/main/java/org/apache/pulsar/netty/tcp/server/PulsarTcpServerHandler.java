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
package org.apache.pulsar.netty.tcp.server;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.netty.serde.PulsarSerializer;
import org.apache.pulsar.netty.common.PulsarUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * Handles a server-side channel
 */
@ChannelHandler.Sharable
public class PulsarTcpServerHandler<T> extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarTcpServerHandler.class);
    private String serviceUrl;
    private String topicName;
    private PulsarSerializer<T> pulsarSerializer;
    private Function<Throwable, MessageId> failureCallback;

    public PulsarTcpServerHandler(String serviceUrl, String topicName, PulsarSerializer<T> pulsarSerializer) {
        this.serviceUrl = serviceUrl;
        this.topicName = topicName;
        this.pulsarSerializer = pulsarSerializer;

        this.failureCallback = cause -> {
            LOG.error("Error while sending record to Pulsar : " + cause.getMessage(), cause);
            return null;
        };
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
            throws Exception {
        T t = (T) msg;
        byte[] dataBytes = this.pulsarSerializer.serialize(t);
        PulsarUtils.getProducerInstance(this.serviceUrl, this.topicName)
            .sendAsync(dataBytes)
            .exceptionally(failureCallback);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Error when processing incoming data", cause);
        ctx.close();
    }

}
