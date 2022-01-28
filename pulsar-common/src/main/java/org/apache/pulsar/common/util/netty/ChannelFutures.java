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
package org.apache.pulsar.common.util.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Static utility methods for operating on {@link ChannelFuture}s.
 *
 */
public class ChannelFutures {

    private ChannelFutures() {
        throw new AssertionError("Class with static utility methods only cannot be instantiated");
    }

    /**
     * Convert a {@link ChannelFuture} into a {@link CompletableFuture}.
     *
     * @param channelFuture the {@link ChannelFuture}
     * @return a {@link CompletableFuture} that completes successfully when the channelFuture completes successfully,
     *         and completes exceptionally if the channelFuture completes with a {@link Throwable}
     */
    public static CompletableFuture<Channel> toCompletableFuture(ChannelFuture channelFuture) {
        Objects.requireNonNull(channelFuture, "channelFuture cannot be null");

        CompletableFuture<Channel> adapter = new CompletableFuture<>();
        if (channelFuture.isDone()) {
            if (channelFuture.isSuccess()) {
                adapter.complete(channelFuture.channel());
            } else {
                adapter.completeExceptionally(channelFuture.cause());
            }
        } else {
            channelFuture.addListener((ChannelFuture cf) -> {
                if (cf.isSuccess()) {
                    adapter.complete(cf.channel());
                } else {
                    adapter.completeExceptionally(cf.cause());
                }
            });
        }
        return adapter;
    }
}

