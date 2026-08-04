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

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.handler.ssl.SslProvider;
import io.opentelemetry.api.OpenTelemetry;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.Test;

/**
 * Unit tests for the PIP-478 {@link TlsFactorySupport} helpers (the only server TLS path since the PIP-337
 * removal, stage 4c).
 */
public class TlsFactorySupportTest {

    @Test
    public void createFactoryUsesDefaultForBlankSentinelOrDefaultClassName() throws Exception {
        PulsarTlsFactory sentinel = new NoOpTlsFactory();
        assertThat(TlsFactorySupport.createFactory("", NoOpTlsFactory.class, () -> sentinel)).isSameAs(sentinel);
        assertThat(TlsFactorySupport.createFactory("default", NoOpTlsFactory.class, () -> sentinel))
                .isSameAs(sentinel);
        assertThat(TlsFactorySupport.createFactory("DEFAULT", NoOpTlsFactory.class, () -> sentinel))
                .isSameAs(sentinel);
        assertThat(TlsFactorySupport.createFactory(NoOpTlsFactory.class.getName(), NoOpTlsFactory.class,
                () -> sentinel)).isSameAs(sentinel);
    }

    @Test
    public void createFactoryInstantiatesNamedCustomClassReflectively() throws Exception {
        PulsarTlsFactory sentinel = new NoOpTlsFactory();
        PulsarTlsFactory created =
                TlsFactorySupport.createFactory(NoOpTlsFactory.class.getName(), null, () -> sentinel);
        // With no defaultFactoryClass to match, the class name is instantiated reflectively (a new instance).
        assertThat(created).isInstanceOf(NoOpTlsFactory.class).isNotSameAs(sentinel);
    }

    @Test
    public void parseFactoryConfigHandlesBlankJsonAndKeyValue() {
        assertThat(TlsFactorySupport.parseFactoryConfig("")).isEmpty();
        assertThat(TlsFactorySupport.parseFactoryConfig(null)).isEmpty();
        assertThat(TlsFactorySupport.parseFactoryConfig("{\"a\":\"1\",\"b\":\"2\"}"))
                .containsEntry("a", "1").containsEntry("b", "2");
        assertThat(TlsFactorySupport.parseFactoryConfig("a=1, b = 2 ,c="))
                .containsEntry("a", "1").containsEntry("b", "2").containsEntry("c", "");
    }

    @Test
    public void engineProviderMapsOnlyExplicitOpenSsl() {
        assertThat(TlsFactorySupport.engineProvider(null)).isEqualTo(SslProvider.JDK);
        assertThat(TlsFactorySupport.engineProvider("Conscrypt")).isEqualTo(SslProvider.JDK);
        assertThat(TlsFactorySupport.engineProvider("SunJSSE")).isEqualTo(SslProvider.JDK);
        assertThat(TlsFactorySupport.engineProvider("openssl")).isEqualTo(SslProvider.OPENSSL);
        assertThat(TlsFactorySupport.engineProvider("OPENSSL_REFCNT")).isEqualTo(SslProvider.OPENSSL);
    }

    @Test
    public void resolveJsseProviderRoutesNonEngineValueAndHonorsExplicit() {
        // An explicit jsseProvider always wins (trimmed).
        assertThat(TlsFactorySupport.resolveJsseProvider("BCJSSE", "OPENSSL")).isEqualTo("BCJSSE");
        assertThat(TlsFactorySupport.resolveJsseProvider(" BCJSSE ", null)).isEqualTo("BCJSSE");
        // Absent an explicit value, a non-engine engineProviderString (a JSSE provider name) is routed here.
        assertThat(TlsFactorySupport.resolveJsseProvider(null, "Conscrypt")).isEqualTo("Conscrypt");
        assertThat(TlsFactorySupport.resolveJsseProvider("", " SunJSSE ")).isEqualTo("SunJSSE");
        // Engine literals stay on the engine axis and yield null here (case-insensitive).
        assertThat(TlsFactorySupport.resolveJsseProvider(null, "JDK")).isNull();
        assertThat(TlsFactorySupport.resolveJsseProvider(null, "openssl")).isNull();
        assertThat(TlsFactorySupport.resolveJsseProvider(null, "OPENSSL_REFCNT")).isNull();
        // Both blank/null yields null.
        assertThat(TlsFactorySupport.resolveJsseProvider(null, null)).isNull();
        assertThat(TlsFactorySupport.resolveJsseProvider("  ", "  ")).isNull();
    }

    @Test
    public void initContextWiresServicesAndDefaultsOpenTelemetry() {
        TlsFactoryInitContext ctx = TlsFactorySupport.initContext(java.util.Map.of("k", "v"), null, null, null);
        assertThat(ctx.params()).containsEntry("k", "v");
        assertThat(ctx.clock()).isNotNull();
        assertThat(ctx.openTelemetry()).isSameAs(OpenTelemetry.noop());
    }

    /** A public no-arg {@link PulsarTlsFactory} for reflective-instantiation testing. */
    public static final class NoOpTlsFactory implements PulsarTlsFactory {
        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                                                                            Class<T> instanceClass) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                                                                            Class<T> instanceClass,
                                                                            Consumer<T> onLoadOrReload) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public void close() {
        }
    }
}
