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

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.socksx.v5.DefaultSocks5PasswordAuthRequest;
import io.netty.handler.codec.socksx.v5.DefaultSocks5PasswordAuthResponse;
import io.netty.handler.codec.socksx.v5.Socks5PasswordAuthStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.socks5.auth.DefaultPasswordAuthImpl;
import org.apache.pulsar.socks5.auth.PasswordAuth;

@Slf4j
public class PasswordAuthRequestHandler extends SimpleChannelInboundHandler<DefaultSocks5PasswordAuthRequest> {

    private final PasswordAuth passwordAuth;

    public PasswordAuthRequestHandler() {
        this.passwordAuth = new DefaultPasswordAuthImpl();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DefaultSocks5PasswordAuthRequest msg) throws Exception {
        if (passwordAuth.auth(msg.username(), msg.password())) {
            ctx.writeAndFlush(new DefaultSocks5PasswordAuthResponse(Socks5PasswordAuthStatus.SUCCESS));
        } else {
            ctx.writeAndFlush(new DefaultSocks5PasswordAuthResponse(Socks5PasswordAuthStatus.FAILURE))
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

}
