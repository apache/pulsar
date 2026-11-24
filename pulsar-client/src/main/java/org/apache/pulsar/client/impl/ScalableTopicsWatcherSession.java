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
package org.apache.pulsar.client.impl;

import java.util.List;
import org.apache.pulsar.common.api.proto.ServerError;

/**
 * Client-side callback for a namespace-wide scalable-topics watch session.
 *
 * <p>The broker pushes either a full {@code Snapshot} (initial subscribe and on every
 * reconnect resync) or an incremental {@code Diff}. Implementations apply the
 * snapshot as a full state replacement and the diff as a set delta — both are
 * idempotent and self-healing across reconnects.
 *
 * <p>Implemented by the V5 client's {@code ScalableTopicsWatcher}.
 */
public interface ScalableTopicsWatcherSession {

    /**
     * Full set of topics currently matching the watch's filters. The implementation
     * should replace any local state derived from prior events with this snapshot.
     */
    void onSnapshot(List<String> topics);

    /**
     * Incremental membership change. Apply {@code removed} before {@code added}
     * (covers a rapid remove-then-add of the same topic name within a coalescing
     * window).
     */
    void onDiff(List<String> added, List<String> removed);

    /**
     * The broker rejected the watch (e.g. authz, broker shutting down).
     * Implementations should fail any pending start future and stop emitting events.
     */
    void onError(ServerError error, String message);

    /**
     * The underlying connection dropped. Implementations should treat any local set
     * as stale until the next snapshot arrives.
     */
    void connectionClosed();
}
