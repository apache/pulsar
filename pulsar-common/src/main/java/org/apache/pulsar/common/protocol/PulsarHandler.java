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
package org.apache.pulsar.common.protocol;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.api.proto.CommandPing;
import org.apache.pulsar.common.api.proto.CommandPong;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the channel handler to process inbound Pulsar data.
 */
public abstract class PulsarHandler extends PulsarDecoder {
    protected ChannelHandlerContext ctx;
    protected SocketAddress remoteAddress;
    private int remoteEndpointProtocolVersion = ProtocolVersion.v0.getValue();
    private final long keepAliveIntervalSeconds;
    private boolean waitingForPingResponse = false;
    private ScheduledFuture<?> keepAliveTask;

    public int getRemoteEndpointProtocolVersion() {
        return remoteEndpointProtocolVersion;
    }

    protected void setRemoteEndpointProtocolVersion(int remoteEndpointProtocolVersion) {
        this.remoteEndpointProtocolVersion = remoteEndpointProtocolVersion;
    }

    public PulsarHandler(int keepAliveInterval, TimeUnit unit) {
        this.keepAliveIntervalSeconds = unit.toSeconds(keepAliveInterval);
    }

    @Override
    protected void messageReceived() {
        waitingForPingResponse = false;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.remoteAddress = ctx.channel().remoteAddress();
        this.ctx = ctx;

        if (log.isDebugEnabled()) {
            log.debug("[{}] Scheduling keep-alive task every {} s", this.toString(), keepAliveIntervalSeconds);
        }
        if (keepAliveIntervalSeconds > 0) {
            this.keepAliveTask = ctx.executor()
                    .scheduleAtFixedRate(catchingAndLoggingThrowables(this::handleKeepAliveTimeout),
                            keepAliveIntervalSeconds, keepAliveIntervalSeconds, TimeUnit.SECONDS);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cancelKeepAliveTask();
    }

    @Override
    protected final void handlePing(CommandPing ping) {
        // Immediately reply success to ping requests
        if (log.isDebugEnabled()) {
            log.debug("[{}] Replying back to ping message", this.toString());
        }
        ctx.writeAndFlush(Commands.newPong())
                .addListener(future -> {
                    if (!future.isSuccess()) {
                        log.warn("[{}] Forcing connection to close since cannot send a pong message.",
                                toString(), future.cause());
                        ctx.close();
                    }
                });
    }

    @Override
    protected final void handlePong(CommandPong pong) {
    }

    private void handleKeepAliveTimeout() {
        if (!ctx.channel().isOpen()) {
            return;
        }

        if (!isHandshakeCompleted()) {
            log.warn("[{}] Pulsar Handshake was not completed within timeout, closing connection", this.toString());
            ctx.close();
        } else if (waitingForPingResponse && ctx.channel().config().isAutoRead()) {
            // We were waiting for a response and another keep-alive just completed.
            // If auto-read was disabled, it means we stopped reading from the connection, so we might receive the Ping
            // response later and thus not enforce the strict timeout here.
            log.warn("[{}] Forcing connection to close after keep-alive timeout", this.toString());
            ctx.close();
        } else if (getRemoteEndpointProtocolVersion() >= ProtocolVersion.v1.getValue()) {
            // Send keep alive probe to peer only if it supports the ping/pong commands, added in v1
            if (log.isDebugEnabled()) {
                log.debug("[{}] Sending ping message", this.toString());
            }
            waitingForPingResponse = true;
            sendPing();
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Peer doesn't support keep-alive", this.toString());
            }
        }
    }

    protected ChannelFuture sendPing() {
        return ctx.writeAndFlush(Commands.newPing())
<<<<<<< HEAD
            .addListener(future -> {
                if (!future.isSuccess()) {
                    log.warn("[{}] Forcing connection to close since cannot send a ping message.",
                            ctx.channel(), future.cause());
                    ctx.close();
                }
            });
=======
                .addListener(future -> {
                    if (!future.isSuccess()) {
                        log.warn("[{}] Forcing connection to close since cannot send a ping message.",
                                this.toString(), future.cause());
                        ctx.close();
                    }
                });
>>>>>>> d77c5de5d7 ([improve] [log] Print source client addr when enabled haProxyProtocolEnabled (#22686))
    }

    public void cancelKeepAliveTask() {
        if (keepAliveTask != null) {
            keepAliveTask.cancel(false);
            keepAliveTask = null;
        }
    }

    /**
     * @return true if the connection is ready to use, meaning the Pulsar handshake was already completed
     */
    protected abstract boolean isHandshakeCompleted();

    /**
     * Demo: [id: 0x2561bcd1, L:/10.0.136.103:6650 ! R:/240.240.0.5:58038].
     * L: local Address.
     * R: remote address.
     */
    @Override
    public String toString() {
        ChannelHandlerContext ctx = this.ctx;
        if (ctx == null) {
            return "[ctx: null]";
        } else {
            return ctx.channel().toString();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PulsarHandler.class);
}
