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
package org.apache.pulsar.common.tls.impl;

import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;

/**
 * Fail-fast probe support for the framework's boot-time contract enforcement (PIP-478).
 *
 * <p>The framework probes every {@code (purpose, class)} pair it needs at startup so that a
 * misconfiguration surfaces as a boot error rather than at first connection. A probe uses the
 * one-shot, endpoint-less {@code createInstance} form and <strong>retains</strong> the returned handle
 * as the initial cached instance — the probe is the first load, not a throwaway.
 */
public final class TlsFactoryProbe {

    private TlsFactoryProbe() {
    }

    /**
     * Eagerly build (probe) the instance of {@code instanceClass} for {@code purpose}, returning the
     * retained handle. Blocks until the factory completes the request.
     *
     * @param factory       the factory to probe
     * @param purpose       the purpose to probe
     * @param instanceClass the well-known instance class to probe
     * @param <T>           the instance type
     * @return the retained handle whose {@link TlsHandle#get()} is the initial instance
     * @throws IllegalStateException if the factory does not support the {@code (purpose, class)}
     *                               combination, or completes the request exceptionally (a boot error)
     */
    public static <T> TlsHandle<T> probe(PulsarTlsFactory factory, TlsPurpose purpose, Class<T> instanceClass) {
        Optional<TlsHandle<T>> handle;
        try {
            handle = factory.createInstance(purpose, instanceClass).join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new IllegalStateException(
                    "Failed to build TLS " + instanceClass.getName() + " for purpose " + purpose, cause);
        }
        return handle.orElseThrow(() -> new IllegalStateException(
                "TLS factory does not support building " + instanceClass.getName() + " for purpose " + purpose));
    }
}
