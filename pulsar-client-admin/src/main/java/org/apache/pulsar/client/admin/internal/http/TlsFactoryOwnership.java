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
package org.apache.pulsar.client.admin.internal.http;

import java.util.concurrent.ScheduledExecutorService;
import lombok.CustomLog;
import org.apache.pulsar.tls.PulsarTlsFactory;

/**
 * A {@link PulsarTlsFactory} together with the responsibility for closing it (PIP-478).
 *
 * <p>The admin client has more than one holder of the same factory: a {@code PulsarAdmin} ends up with two
 * {@link AsyncHttpConnector} instances — the one {@code PulsarAdminImpl} builds and the one Jersey creates
 * lazily on the first request — and the factory may in turn have been handed in from outside, by the broker's
 * admin attach. Every one of those holders needs the factory; exactly one may close it. Carrying the
 * factory and that responsibility together makes the handoff say which one it is, so a holder cannot acquire
 * a reference without also learning whether closing is its job.
 *
 * <p>{@link #close()} is therefore always safe to call: it releases the factory and its rotation executor
 * when this handle {@linkplain #owning owns} them, and does nothing when it is only {@linkplain #borrowing
 * borrowing} them.
 */
@CustomLog
final class TlsFactoryOwnership implements AutoCloseable {

    private static final TlsFactoryOwnership NONE = new TlsFactoryOwnership(null, null, false);

    private final PulsarTlsFactory factory;
    private final ScheduledExecutorService executor;
    private final boolean owned;

    private TlsFactoryOwnership(PulsarTlsFactory factory, ScheduledExecutorService executor, boolean owned) {
        this.factory = factory;
        this.executor = executor;
        this.owned = owned;
    }

    /**
     * @return a handle on no factory at all — this configuration needs none
     */
    static TlsFactoryOwnership none() {
        return NONE;
    }

    /**
     * Take responsibility for a factory this holder resolved itself, and for the executor driving its
     * material rotation. {@link #close()} releases both.
     *
     * @param factory  the resolved factory
     * @param executor the executor driving its rotation, released with it
     * @return an owning handle
     */
    static TlsFactoryOwnership owning(PulsarTlsFactory factory, ScheduledExecutorService executor) {
        return factory == null ? NONE : new TlsFactoryOwnership(factory, executor, true);
    }

    /**
     * Use a factory somebody else owns and will close — here, the one
     * {@link AsyncHttpConnectorProvider} shares across the connectors of a single {@code PulsarAdmin}.
     * {@link #close()} does nothing.
     *
     * @param factory the borrowed factory, or {@code null} for {@link #none()}
     * @return a borrowing handle
     */
    static TlsFactoryOwnership borrowing(PulsarTlsFactory factory) {
        return factory == null ? NONE : new TlsFactoryOwnership(factory, null, false);
    }

    /**
     * @return the factory, or {@code null} when there is none
     */
    PulsarTlsFactory factory() {
        return factory;
    }

    /**
     * @return whether this handle carries a factory
     */
    boolean isPresent() {
        return factory != null;
    }

    @Override
    public void close() {
        if (!owned) {
            return;
        }
        try {
            factory.close();
        } catch (Exception e) {
            log.warn().exception(e).log("Failed to close the admin client TLS factory");
        } finally {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }
}
