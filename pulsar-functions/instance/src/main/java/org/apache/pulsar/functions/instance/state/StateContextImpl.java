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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * This class accumulates the state updates from one function.
 *
 * <p>currently it exposes incr operations. but we can expose other key/values operations if needed.
 */
public class StateContextImpl implements StateContext {

    private final Table<ByteBuf, ByteBuf> table;
    // the list
    private final List<CompletableFuture<Void>> updates;

    public StateContextImpl(Table<ByteBuf, ByteBuf> table) {
        this.table = table;
        this.updates = new ArrayList<>();
    }

    @Override
    public void incr(String key, long amount) {
        // TODO: this can be optimized with a batch operation.
        updates.add(table.increment(
            Unpooled.wrappedBuffer(key.getBytes(UTF_8)),
            amount));
    }

    /**
     * flush and wait all the updates to be completed.
     */
    @Override
    public CompletableFuture<Void> flush() {
        return FutureUtils.collect(updates).thenApply(ignored -> null);
    }
}
