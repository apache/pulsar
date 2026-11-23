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
package org.apache.pulsar.tests.integration.messaging;

import io.netty.bootstrap.ChannelInitializerExtension;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.common.protocol.ByteBufPair;

/** Inject a single bad checksum on the source broker's client connection for each marked E2E message. */
public final class ReplicationChecksumFault extends ChannelInitializerExtension {
    static final String WIRE = "pip-502-checksum-wire/";
    static final String RETAINED = "pip-502-checksum-retained/";
    private static final Set<String> INJECTED = ConcurrentHashMap.newKeySet();

    @Override
    public void postInitializeClientChannel(Channel channel) {
        channel.pipeline().addLast(new Initializer());
    }

    static final class Initializer extends ChannelInitializer<Channel> {
        @Override
        protected void initChannel(Channel channel) {
            var pipeline = channel.pipeline();
            var client = pipeline.context(ClientCnx.class);
            if (client != null) {
                pipeline.addBefore(client.name(), "replicationChecksumFault", new OutboundHandler());
            }
        }
    }

    static final class OutboundHandler extends ChannelOutboundHandlerAdapter {
        @Override
        public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) {
            if (message instanceof ByteBufPair frame) {
                ByteBuf payload = frame.getSecond();
                // Only inspect unbatched marker payloads, which contain the prefix and a UUID.
                int length = payload.readableBytes();
                if (length == WIRE.length() + 36 || length == RETAINED.length() + 36) {
                    String marker = payload.toString(StandardCharsets.US_ASCII);
                    boolean wireOnly = marker.startsWith(WIRE);
                    if ((wireOnly || marker.startsWith(RETAINED)) && INJECTED.add(marker)) {
                        if (wireOnly) {
                            // The producer retains the clean original for local verification and resend.
                            var copy = ByteBufPair.get(frame.getFirst().copy(), payload.retainedDuplicate());
                            frame.release();
                            frame = copy;
                        }
                        // [total size][command size][command][CRC magic][CRC32C]
                        ByteBuf header = frame.getFirst();
                        int commandSize = header.getInt(header.readerIndex() + Integer.BYTES);
                        int checksumOffset = header.readerIndex() + 2 * Integer.BYTES + commandSize + Short.BYTES;
                        header.setInt(checksumOffset, header.getInt(checksumOffset) ^ 1);
                        message = frame;
                    }
                }
            }
            ctx.write(message, promise);
        }
    }
}
