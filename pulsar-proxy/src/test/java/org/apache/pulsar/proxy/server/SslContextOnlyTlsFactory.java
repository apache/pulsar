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
package org.apache.pulsar.proxy.server;

import io.netty.handler.ssl.SslContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactory;
import org.apache.pulsar.common.tls.impl.FileBasedTlsFactorySettings;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsEndpoint;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;

/**
 * A PIP-478 test factory that supplies <em>only</em> the JDK {@code javax.net.ssl.SSLContext} for every
 * purpose — it always returns {@link Optional#empty()} for the Netty {@code io.netty.handler.ssl.SslContext}
 * — forcing the framework's {@code SSLContext}-fallback synthesis. It delegates the actual material
 * loading to a {@link FileBasedTlsFactory} so it exercises the real (rotating) load path.
 *
 * <p>Reflectively instantiable via the no-arg constructor (selected by {@code tlsFactoryClassName} /
 * {@code brokerClientTlsFactoryClassName}, reading {@code trust}/{@code cert}/{@code key}/{@code insecure}
 * from the factory config params), and directly constructible for adoption through the v5 client builder's
 * {@code tlsFactory(...)}.
 *
 * <p>{@link #nettyRequestCount()} counts the {@code SslContext} requests it refused, so a test can assert the
 * synthesis path was actually taken rather than silently skipped.
 */
public class SslContextOnlyTlsFactory implements PulsarTlsFactory {

    private static final List<TlsPurpose> ALL_PURPOSES = List.of(
            TlsPurpose.BROKER, TlsPurpose.PROXY, TlsPurpose.WEB,
            TlsPurpose.BROKER_CLIENT, TlsPurpose.CLIENT_DEFAULT, TlsPurpose.CLIENT_OAUTH2);

    private static final AtomicInteger NETTY_REQUESTS = new AtomicInteger();

    private volatile String trustCertsFilePath;
    private volatile String certificateFilePath;
    private volatile String keyFilePath;
    private volatile boolean allowInsecureConnection;
    private volatile FileBasedTlsFactory delegate;

    /** Reflective (class-name) constructor: material comes from the factory config params. */
    public SslContextOnlyTlsFactory() {
    }

    /** Direct constructor for v5 client adoption: material is supplied explicitly. */
    public SslContextOnlyTlsFactory(String trustCertsFilePath, String certificateFilePath, String keyFilePath,
                                    boolean allowInsecureConnection) {
        this.trustCertsFilePath = trustCertsFilePath;
        this.certificateFilePath = certificateFilePath;
        this.keyFilePath = keyFilePath;
        this.allowInsecureConnection = allowInsecureConnection;
    }

    /** Total number of Netty {@code SslContext} requests this factory refused (across all instances). */
    public static int nettyRequestCount() {
        return NETTY_REQUESTS.get();
    }

    public static void resetNettyRequestCount() {
        NETTY_REQUESTS.set(0);
    }

    @Override
    public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
        Map<String, String> params = context.params();
        if (trustCertsFilePath == null) {
            trustCertsFilePath = StringUtils.trimToNull(params.get("trust"));
        }
        if (certificateFilePath == null) {
            certificateFilePath = StringUtils.trimToNull(params.get("cert"));
        }
        if (keyFilePath == null) {
            keyFilePath = StringUtils.trimToNull(params.get("key"));
        }
        if (params.containsKey("insecure")) {
            allowInsecureConnection = Boolean.parseBoolean(params.get("insecure"));
        }
        TlsPolicy policy = TlsPolicy.builder()
                .format(TlsPolicy.Format.PEM)
                .trustCertsFilePath(trustCertsFilePath)
                .certificateFilePath(certificateFilePath)
                .keyFilePath(keyFilePath)
                .allowInsecureConnection(allowInsecureConnection)
                .enableHostnameVerification(false)
                .build();
        Map<TlsPurpose, TlsPolicy> policies = new LinkedHashMap<>();
        for (TlsPurpose purpose : ALL_PURPOSES) {
            policies.put(purpose, policy);
        }
        this.delegate = new FileBasedTlsFactory(policies, FileBasedTlsFactorySettings.builder().build());
        return delegate.initialize(context);
    }

    @Override
    public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose, Class<T> instanceClass) {
        if (instanceClass == SslContext.class) {
            NETTY_REQUESTS.incrementAndGet();
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return delegate.createInstance(purpose, instanceClass);
    }

    @Override
    public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
            TlsPurpose purpose, TlsEndpoint endpoint, Class<T> instanceClass) {
        if (instanceClass == SslContext.class) {
            NETTY_REQUESTS.incrementAndGet();
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return delegate.createInstance(purpose, endpoint, instanceClass);
    }

    @Override
    public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
            TlsPurpose purpose, Class<T> instanceClass, Consumer<T> onLoadOrReload) {
        if (instanceClass == SslContext.class) {
            NETTY_REQUESTS.incrementAndGet();
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return delegate.createInstance(purpose, instanceClass, onLoadOrReload);
    }

    @Override
    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }
}
