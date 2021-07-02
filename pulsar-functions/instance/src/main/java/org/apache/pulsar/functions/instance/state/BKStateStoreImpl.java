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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.api.kv.options.Options;
import org.apache.pulsar.functions.api.StateStoreContext;
import org.apache.pulsar.functions.utils.FunctionCommon;

/**
 * This class accumulates the state updates from one function.
 *
 * <p>currently it exposes incr operations. but we can expose other key/values operations if needed.
 */
public class BKStateStoreImpl implements DefaultStateStore {

    private final String tenant;
    private final String namespace;
    private final String name;
    private final String fqsn;
    private final Table<ByteBuf, ByteBuf> table;

    public BKStateStoreImpl(String tenant, String namespace, String name,
                            Table<ByteBuf, ByteBuf> table) {
        this.tenant = tenant;
        this.namespace = namespace;
        this.name = name;
        this.table = table;
        this.fqsn = FunctionCommon.getFullyQualifiedName(tenant, namespace, name);
    }

    @Override
    public String tenant() {
        return tenant;
    }

    @Override
    public String namespace() {
        return namespace;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String fqsn() {
        return fqsn;
    }

    @Override
    public void init(StateStoreContext ctx) {
    }

    @Override
    public void close() {
        table.close();
    }

    @Override
    public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
        // TODO: this can be optimized with a batch operation.
        return table.increment(
            Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
            amount);
    }

    @Override
    public void incrCounter(String key, long amount) {
        try {
            result(incrCounterAsync(key, amount));
        } catch (Exception e) {
            throw new RuntimeException("Failed to increment key '" + key + "' by amount '" + amount + "'", e);
        }
    }

    @Override
    public CompletableFuture<Long> getCounterAsync(String key) {
        return table.getNumber(Unpooled.wrappedBuffer(key.getBytes(UTF_8)));
    }

    @Override
    public long getCounter(String key) {
        try {
            return result(getCounterAsync(key));
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve counter from key '" + key + "'");
        }
    }

    @Override
    public CompletableFuture<Void> putAsync(String key, ByteBuffer value) {
        if(value != null) {
            // Set position to off the buffer to the beginning.
            // If a user used an operation like ByteBuffer.allocate(4).putInt(count) to create a ByteBuffer to store to the state store
            // the position of the buffer will be at the end and nothing will be written to table service
            value.position(0);
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
    public void put(String key, ByteBuffer value) {
        try {
            result(putAsync(key, value));
        } catch (Exception e) {
            throw new RuntimeException("Failed to update the state value for key '" + key + "'");
        }
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String key) {
        return table.delete(
                Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
                Options.delete()
        ).thenApply(ignored -> null);
    }

    @Override
    public void delete(String key) {
        try {
            result(deleteAsync(key));
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete the state value for key '" + key + "'");
        }
    }

    @Override
    public CompletableFuture<ByteBuffer> getAsync(String key) {
        return table.get(Unpooled.wrappedBuffer(key.getBytes(UTF_8))).thenApply(
                data -> {
                    try {
                        if (data != null) {
                            ByteBuffer result = ByteBuffer.allocate(data.readableBytes());
                            data.readBytes(result);
                            // Set position to off the buffer to the beginning, since the position after the read is going to be end of the buffer
                            // If we do not rewind to the begining here, users will have to explicitly do this in their function code
                            // in order to use any of the ByteBuffer operations
                            result.position(0);
                            return result;
                        }
                        return null;
                    } finally {
                        if (data != null) {
                            ReferenceCountUtil.safeRelease(data);
                        }
                    }
                }
        );
    }

    @Override
    public ByteBuffer get(String key) {
        try {
            return result(getAsync(key));
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve the state value for key '" + key + "'", e);
        }
    }
}
