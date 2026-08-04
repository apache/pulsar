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
package org.apache.pulsar.tls;

/**
 * A handle for a built TLS instance, returned by every {@link PulsarTlsFactory} {@code createInstance}
 * form (PIP-478).
 *
 * <p>For a one-shot request, {@link #get()} returns the built instance. For a subscribing request, it
 * returns the value most recently delivered to the reload callback (the initial load, or the latest
 * rebuild), so a consumer can read the live instance on demand without caching callback deliveries.
 *
 * <p><b>Instance ownership.</b> The value returned by {@link #get()} is a <em>factory-owned
 * snapshot</em>: consumers never close, release, or mutate it — in particular they must not
 * {@code release()} a Netty reference-counted OpenSSL context. {@link #dispose()} only signals that
 * this consumer is done; the factory releases a superseded or fully-disposed instance's native
 * resources itself, once the last handle referencing it is gone.
 *
 * @param <T> the built instance type (e.g. {@code io.netty.handler.ssl.SslContext},
 *            {@code javax.net.ssl.SSLContext}, or Jetty's {@code SslContextFactory.Server})
 */
public interface TlsHandle<T> {

    /**
     * Returns the current instance snapshot. This <b>never blocks</b>: for a one-shot request it returns
     * the already-built instance; for a subscribing request the initial value is present before this
     * method is first callable, because the {@code createInstance} future completes only after the first
     * reload delivery (see {@link PulsarTlsFactory}). Calling {@code get()} after {@link #dispose()} is
     * unspecified — the factory may already have released the instance's backing resources — so a consumer
     * MUST stop calling {@code get()} once it has disposed the handle.
     *
     * @return the built instance for a one-shot request, or the most recently delivered instance for
     *         a subscribing request (never {@code null}); treated by consumers as an immutable borrow
     */
    T get();

    /**
     * Unregister the reload callback (if any) and release the factory-side resources backing this
     * handle (background refresh, caches). Signals only that this consumer is done; the factory owns
     * the built instance's lifecycle. <b>Idempotent</b>: only the first call has effect; subsequent
     * calls are no-ops and never double-release the instance.
     */
    void dispose();
}
