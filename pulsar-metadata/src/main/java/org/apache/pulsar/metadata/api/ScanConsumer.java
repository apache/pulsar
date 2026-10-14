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
package org.apache.pulsar.metadata.api;

/**
 * Streaming consumer for {@link MetadataStore#scanChildren} and {@link MetadataStore#scanByIndex}
 * results.
 *
 * <p>The store invokes {@link #onNext} for each record (in key order), and then either
 * {@link #onCompleted} (success) or {@link #onError} (failure) exactly once. Implementations must
 * be safe to invoke from a metadata-store internal thread; back-pressure is the consumer's
 * responsibility (long blocking work in {@code onNext} can stall the scan).
 */
public interface ScanConsumer {

    /**
     * Called once per record. The result's {@link Stat#getPath()} carries the full key.
     *
     * @param result a child record under the requested parent path
     */
    void onNext(GetResult result);

    /**
     * Called at most once when the scan fails. After this call no further callbacks are made.
     *
     * @param throwable the cause of the failure
     */
    void onError(Throwable throwable);

    /**
     * Called at most once when the scan finishes without error. After this call no further
     * callbacks are made.
     */
    void onCompleted();

    /**
     * A {@link ScanConsumer} that accumulates emitted records into the given list. Useful when a
     * caller wants the convenience of a {@code CompletableFuture<List<GetResult>>} on top of the
     * streaming API — failure / completion are signaled by the future returned from
     * {@code scanByIndex} / {@code scanChildren}.
     *
     * <pre>{@code
     * List<GetResult> out = new ArrayList<>();
     * store.scanByIndex(prefix, indexName, key, key, filter, ScanConsumer.collectInto(out)).join();
     * }</pre>
     */
    static ScanConsumer collectInto(java.util.List<GetResult> out) {
        return new ScanConsumer() {
            @Override
            public void onNext(GetResult result) {
                out.add(result);
            }

            @Override
            public void onError(Throwable throwable) {
                // Caller observes failure via the scan's CompletableFuture.
            }

            @Override
            public void onCompleted() {
                // No-op.
            }
        };
    }
}
