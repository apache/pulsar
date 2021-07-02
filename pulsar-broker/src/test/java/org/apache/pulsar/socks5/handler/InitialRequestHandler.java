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
package org.apache.pulsar.socks5.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.socksx.SocksVersion;
import io.netty.handler.codec.socksx.v5.DefaultSocks5InitialRequest;
import io.netty.handler.codec.socksx.v5.DefaultSocks5InitialResponse;
import io.netty.handler.codec.socksx.v5.Socks5AuthMethod;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.socks5.config.Socks5Config;

@Slf4j
public class InitialRequestHandler extends SimpleChannelInboundHandler<DefaultSocks5InitialRequest> {

    private final Socks5Config socks5Config;

    public InitialRequestHandler(final Socks5Config socks5Config) {
        this.socks5Config = socks5Config;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DefaultSocks5InitialRequest msg) throws Exception {
        if (SocksVersion.SOCKS5.equals(msg.version())) {
            if (msg.decoderResult().isFailure()) {
                log.warn("decode failure : {}", msg.decoderResult());
                ctx.fireChannelRead(msg);
            } else {
                if (socks5Config.isEnableAuth()) {
                    ctx.writeAndFlush(new DefaultSocks5InitialResponse(Socks5AuthMethod.PASSWORD));
                } else {
                    ctx.writeAndFlush(new DefaultSocks5InitialResponse(Socks5AuthMethod.NO_AUTH));
                }
            }
        }
    }

}
