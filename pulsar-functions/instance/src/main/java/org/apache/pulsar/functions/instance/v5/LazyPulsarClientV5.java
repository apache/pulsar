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
package org.apache.pulsar.functions.instance.v5;

import java.util.function.Supplier;
import lombok.CustomLog;
import org.apache.pulsar.client.api.v5.PulsarClient;
import org.apache.pulsar.client.api.v5.PulsarClientBuilder;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.internal.PulsarClientProvider;

/**
 * The V5 client of a runtime factory, created on first use.
 *
 * <p>The runtime shares one V5 client across the instances it runs, like the v4 client, but only creates it
 * once a component that uses the V5 client, or user code asking for it, needs it. The V5 client cannot share the
 * v4 client's connections, so a runtime that never runs a V5 component does not pay for a second connection
 * pool.
 */
@CustomLog
public class LazyPulsarClientV5 implements Supplier<PulsarClient>, AutoCloseable {

    /** Creates a configured V5 client builder. */
    @FunctionalInterface
    public interface BuilderFactory {
        PulsarClientBuilder newBuilder() throws PulsarClientException;
    }

    private final BuilderFactory builderFactory;
    private PulsarClient client;
    private boolean closed;

    public LazyPulsarClientV5(BuilderFactory builderFactory) {
        this.builderFactory = builderFactory;
        // The V5 API finds its implementation with a ServiceLoader on the thread's context classloader, once per
        // JVM. In the process and Kubernetes runtimes, user code runs with a context classloader that sees the V5
        // API but not its implementation, so the first V5 call from user code would fail the lookup for good.
        // Look it up now, with the classloader of the runtime.
        withRuntimeClassLoader(() -> {
            try {
                PulsarClientProvider.get();
            } catch (Throwable t) {
                log.warn().exception(t).log("The V5 Pulsar client implementation is not available");
            }
            return null;
        });
    }

    private static <R> R withRuntimeClassLoader(Supplier<R> action) {
        Thread thread = Thread.currentThread();
        ClassLoader contextClassLoader = thread.getContextClassLoader();
        thread.setContextClassLoader(LazyPulsarClientV5.class.getClassLoader());
        try {
            return action.get();
        } finally {
            thread.setContextClassLoader(contextClassLoader);
        }
    }

    /**
     * Returns the shared V5 client, creating it on the first call.
     *
     * @throws IllegalStateException if the client cannot be created or this holder is closed
     */
    @Override
    public synchronized PulsarClient get() {
        if (closed) {
            throw new IllegalStateException("The V5 Pulsar client is closed");
        }
        if (client == null) {
            // user code may be the first to ask for the client, with its own context classloader
            client = withRuntimeClassLoader(() -> {
                try {
                    return builderFactory.newBuilder().build();
                } catch (PulsarClientException e) {
                    throw new IllegalStateException("Failed to create the V5 Pulsar client", e);
                }
            });
        }
        return client;
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (client != null) {
            try {
                client.close();
            } catch (PulsarClientException e) {
                log.warn().exception(e).log("Failed to close the V5 Pulsar client");
            }
            client = null;
        }
    }
}
