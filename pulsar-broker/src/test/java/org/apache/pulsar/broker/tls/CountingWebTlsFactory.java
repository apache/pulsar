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
package org.apache.pulsar.broker.tls;

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
 * A PIP-478 test factory selected via {@code tlsFactoryClassName} that counts every {@code createInstance}
 * call so a test can prove the new TLS SPI was actually exercised for the WEB (HTTPS) purpose — the Jetty web
 * path requests the JDK {@code javax.net.ssl.SSLContext} and {@code SSLParameters}, so unlike
 * {@code SslContextOnlyTlsFactory} (which counts only refused Netty {@code SslContext} requests) this counts
 * the JDK-context path. Material loading is delegated to a real {@link FileBasedTlsFactory} built from the
 * {@code trust}/{@code cert}/{@code key}/{@code insecure} factory-config params, so the served context is a
 * genuine, usable WEB context.
 *
 * <p>Reflectively instantiable via the public no-arg constructor.
 */
public class CountingWebTlsFactory implements PulsarTlsFactory {

    private static final AtomicInteger CREATE_INSTANCE_CALLS = new AtomicInteger();

    private volatile FileBasedTlsFactory delegate;

    public CountingWebTlsFactory() {
    }

    /** Total number of {@code createInstance} calls served (across all instances). */
    public static int createInstanceCount() {
        return CREATE_INSTANCE_CALLS.get();
    }

    public static void reset() {
        CREATE_INSTANCE_CALLS.set(0);
    }

    @Override
    public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
        Map<String, String> params = context.params();
        TlsPolicy policy = TlsPolicy.builder()
                .format(TlsPolicy.Format.PEM)
                .trustCertsFilePath(StringUtils.trimToNull(params.get("trust")))
                .certificateFilePath(StringUtils.trimToNull(params.get("cert")))
                .keyFilePath(StringUtils.trimToNull(params.get("key")))
                .allowInsecureConnection(Boolean.parseBoolean(params.getOrDefault("insecure", "false")))
                .enableHostnameVerification(false)
                .build();
        this.delegate = new FileBasedTlsFactory(Map.of(TlsPurpose.WEB, policy),
                FileBasedTlsFactorySettings.builder().build());
        return delegate.initialize(context);
    }

    @Override
    public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose, Class<T> instanceClass) {
        CREATE_INSTANCE_CALLS.incrementAndGet();
        return delegate.createInstance(purpose, instanceClass);
    }

    @Override
    public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
            TlsPurpose purpose, TlsEndpoint endpoint, Class<T> instanceClass) {
        CREATE_INSTANCE_CALLS.incrementAndGet();
        return delegate.createInstance(purpose, endpoint, instanceClass);
    }

    @Override
    public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(
            TlsPurpose purpose, Class<T> instanceClass, Consumer<T> onLoadOrReload) {
        CREATE_INSTANCE_CALLS.incrementAndGet();
        return delegate.createInstance(purpose, instanceClass, onLoadOrReload);
    }

    @Override
    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }
}
