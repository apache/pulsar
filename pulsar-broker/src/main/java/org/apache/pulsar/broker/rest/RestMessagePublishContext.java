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
package org.apache.pulsar.broker.rest;

import io.netty.util.Recycler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Topic;

/**
 * PublishContext implementation for REST message publishing.
 */
@Slf4j
public class RestMessagePublishContext implements Topic.PublishContext {

    private Topic topic;
    private long startTimeNs;
    private CompletableFuture<PositionImpl> positionFuture;

    /**
     * Executed from managed ledger thread when the message is persisted.
     */
    @Override
    public void completed(Exception exception, long ledgerId, long entryId) {
        if (exception != null) {
            positionFuture.completeExceptionally(exception);
            if (log.isInfoEnabled()) {
                log.info("Failed to write entry for rest produce request: ledgerId: {}, entryId: {}. "
                                + "triggered send callback.",
                        ledgerId, entryId);
            }
        } else {
            if (log.isInfoEnabled()) {
                log.info("Success write topic for rest produce request: {}, ledgerId: {}, entryId: {}. "
                                + "triggered send callback.",
                        topic.getName(), ledgerId, entryId);
            }
            topic.recordAddLatency(System.nanoTime() - startTimeNs, TimeUnit.MICROSECONDS);
            positionFuture.complete(PositionImpl.get(ledgerId, entryId));
        }
        recycle();
    }

    // recycler
    public static RestMessagePublishContext get(CompletableFuture<PositionImpl> positionFuture, Topic topic,
                                                     long startTimeNs) {
        RestMessagePublishContext callback = RECYCLER.get();
        callback.positionFuture = positionFuture;
        callback.topic = topic;
        callback.startTimeNs = startTimeNs;
        return callback;
    }

    private final Recycler.Handle<RestMessagePublishContext> recyclerHandle;

    private RestMessagePublishContext(Recycler.Handle<RestMessagePublishContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<RestMessagePublishContext> RECYCLER = new Recycler<RestMessagePublishContext>() {
        protected RestMessagePublishContext newObject(Handle<RestMessagePublishContext> handle) {
            return new RestMessagePublishContext(handle);
        }
    };

    public void recycle() {
        topic = null;
        startTimeNs = -1;
        recyclerHandle.recycle(this);
    }
}
