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
package org.apache.pulsar.functions.sink;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.connect.core.RecordContext;
import org.apache.pulsar.connect.core.Sink;

/**
 * The default implementation of runtime sink.
 *
 * @param <T>
 */
public class DefaultRuntimeSink<T> implements RuntimeSink<T> {

    public static <T> DefaultRuntimeSink<T> of(Sink<T> sink) {
        return new DefaultRuntimeSink<>(sink);
    }

    private final Sink<T> sink;

    private DefaultRuntimeSink(Sink<T> sink) {
        this.sink = sink;
    }

    /**
     * Open connector with configuration
     *
     * @param config initialization config
     * @throws Exception IO type exceptions when opening a connector
     */
    @Override
    public void open(final Map<String, Object> config) throws Exception {
        sink.open(config);
    }

    /**
     * Attempt to publish a type safe collection of messages
     *
     * @param value output value
     * @return Completable future fo async publish request
     */
    @Override
    public CompletableFuture<Void> write(T value) {
        return sink.write(value);
    }

    @Override
    public void close() throws Exception {
        sink.close();
    }
}
