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
package org.apache.pulsar.functions.instance.state;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.api.kv.options.DeleteOption;
import org.apache.bookkeeper.api.kv.options.Option;
import org.apache.bookkeeper.api.kv.options.Options;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class accumulates the state updates from one function.
 *
 * <p>currently it exposes incr operations. but we can expose other key/values operations if needed.
 */
public class StateContextImpl implements StateContext {

    private final Table<ByteBuf, ByteBuf> table;

    public StateContextImpl(Table<ByteBuf, ByteBuf> table) {
        this.table = table;
    }

    @Override
    public CompletableFuture<Void> incrCounter(String key, long amount) {
        // TODO: this can be optimized with a batch operation.
        return table.increment(
            Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
            amount);
    }

    @Override
    public CompletableFuture<Void> put(String key, ByteBuffer value) {
        if(value != null) {
            return table.put(
                    Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
                    Unpooled.wrappedBuffer(value));
        } else {
            return table.put(
                    Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
                    null);
        }
    }

    @Override
    public CompletableFuture<Void> delete(String key) {
        return table.delete(
                Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
                Options.delete()
        ).thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<ByteBuffer> get(String key) {
        return table.get(Unpooled.wrappedBuffer(key.getBytes(UTF_8))).thenApply(
                data -> {
                    try {
                        if (data != null) {
                            ByteBuffer result = ByteBuffer.allocate(data.readableBytes());
                            data.readBytes(result);
                            return result;
                        }
                        return null;
                    } finally {
                        ReferenceCountUtil.safeRelease(data);
                    }
                }
        );
    }

    @Override
    public CompletableFuture<Long> getCounter(String key) {
        return table.getNumber(Unpooled.wrappedBuffer(key.getBytes(UTF_8)));
    }

}
