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
package org.apache.pulsar.broker.service.streamingdispatch;

import io.netty.util.Recycler;
import lombok.Data;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.PositionImpl;

/**
 * Representing a pending read request to read an entry from {@link ManagedLedger} carrying necessary context.
 */
@Data
public class PendingReadEntryRequest {

    private static final Recycler<PendingReadEntryRequest> RECYCLER = new Recycler<PendingReadEntryRequest>() {
        protected PendingReadEntryRequest newObject(Recycler.Handle<PendingReadEntryRequest> handle) {
            return new PendingReadEntryRequest(handle);
        }
    };

    public static PendingReadEntryRequest create(Object ctx, PositionImpl position) {
        PendingReadEntryRequest pendingReadEntryRequest = RECYCLER.get();
        pendingReadEntryRequest.ctx = ctx;
        pendingReadEntryRequest.position = position;
        pendingReadEntryRequest.retry = 0;
        pendingReadEntryRequest.isLast = false;
        return pendingReadEntryRequest;
    }

    public void recycle() {
        entry = null;
        ctx = null;
        position = null;
        retry = -1;
        recyclerHandle.recycle(this);
    }

    public boolean isLastRequest() {
        return isLast;
    }

    private final Recycler.Handle<PendingReadEntryRequest> recyclerHandle;

    // Entry read from ledger
    public Entry entry;

    // Passed in context that'll be pass to callback
    public Object ctx;

    // Position of entry to be read
    public PositionImpl position;

    // Number of time request has been retried.
    int retry;

    // If request is the last one of a set of requests.
    boolean isLast;
}
