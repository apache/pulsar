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
package org.apache.pulsar.client.impl.v5;

import java.util.concurrent.CompletableFuture;

/**
 * Where a per-segment receive loop deposits a freshly-arrived message.
 *
 * <p>The returned future is the backpressure signal: it completes when the sink is ready
 * to accept the next message. Producers gate re-arming their {@code receiveAsync()} loop
 * on it, so when the downstream (multiplexed) queue fills up, the per-segment loops stop
 * pulling — which lets the underlying v4 consumer's {@code receiverQueueSize} fill and
 * stop issuing flow permits, applying backpressure all the way back to the broker.
 *
 * <p>The default sink is {@link V5ReceiveQueue#offer}; the multi-topic wrappers inject a
 * sink that forwards into their shared mux so its fullness pauses every per-topic loop.
 */
@FunctionalInterface
interface MessageSink<T> {
    CompletableFuture<Void> accept(MessageV5<T> message);
}
