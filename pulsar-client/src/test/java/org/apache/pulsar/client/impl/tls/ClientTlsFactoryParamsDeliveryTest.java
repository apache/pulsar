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
package org.apache.pulsar.client.impl.tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478 stage 4b: a custom (non-default) TLS factory named by {@code brokerClientTlsFactoryClassName}
 * receives its {@code brokerClientTlsFactoryConfig} parameters through {@link TlsFactoryInitContext#params()}.
 * The broker parses that config into {@code ClientConfigurationData.tlsFactoryParams}; this asserts the
 * client-side delivery of that map into the init context a resolved factory is initialized with, using a
 * params-capturing fake factory.
 */
public class ClientTlsFactoryParamsDeliveryTest {

    private ScheduledExecutorService executor;

    @BeforeMethod
    public void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        executor.shutdownNow();
    }

    @Test
    public void deliversTlsFactoryParamsToTheInitContext() throws Exception {
        ParamsCapturingTlsFactory fake = new ParamsCapturingTlsFactory();
        ClientConfigurationData conf = newTlsConf(fake);
        conf.setTlsFactoryParams(Map.of("issuerUrl", "https://idp.example", "region", "eu"));

        PulsarTlsFactory resolved =
                ClientTlsFactorySupport.resolveClientTlsFactory(conf, executor, executor, null);

        assertThat(resolved).as("the adopted factory is returned as-is").isSameAs(fake);
        assertThat(fake.capturedParams)
                .as("the broker-delivered brokerClientTlsFactoryConfig params reach the custom factory")
                .containsEntry("issuerUrl", "https://idp.example")
                .containsEntry("region", "eu");
    }

    @Test
    public void emptyParamsWhenUnset() throws Exception {
        ParamsCapturingTlsFactory fake = new ParamsCapturingTlsFactory();
        ClientConfigurationData conf = newTlsConf(fake);

        ClientTlsFactorySupport.resolveClientTlsFactory(conf, executor, executor, null);

        assertThat(fake.capturedParams).as("no params delivered => empty map (never null)").isEmpty();
    }

    private static ClientConfigurationData newTlsConf(PulsarTlsFactory factory) {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar+ssl://localhost:6651");
        conf.setTlsFactory(factory);
        return conf;
    }

    // PIP-478 Part C: the v4 by-name selector tlsFactoryClassName instantiates a custom PulsarTlsFactory
    // reflectively (no v5-builder instance/policy map set) and parses tlsFactoryConfig JSON into its init params.
    @Test
    public void byNameFactoryInstantiatedReflectivelyWithParsedJsonConfig() throws Exception {
        ByNameCapturingTlsFactory.capturedParams = null;
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar+ssl://localhost:6651");
        conf.setTlsFactoryClassName(ByNameCapturingTlsFactory.class.getName());
        conf.setTlsFactoryConfig("{\"issuerUrl\":\"https://idp.example\",\"region\":\"eu\"}");

        PulsarTlsFactory resolved =
                ClientTlsFactorySupport.resolveClientTlsFactory(conf, executor, executor, null);

        assertThat(resolved).as("the by-name factory is instantiated reflectively")
                .isInstanceOf(ByNameCapturingTlsFactory.class);
        assertThat(ByNameCapturingTlsFactory.capturedParams)
                .as("tlsFactoryConfig JSON is parsed into the factory init params")
                .containsEntry("issuerUrl", "https://idp.example")
                .containsEntry("region", "eu");
    }

    @Test
    public void byNameFactoryParsesKeyValueConfig() throws Exception {
        ByNameCapturingTlsFactory.capturedParams = null;
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar+ssl://localhost:6651");
        conf.setTlsFactoryClassName(ByNameCapturingTlsFactory.class.getName());
        conf.setTlsFactoryConfig("issuerUrl=https://idp.example, region = eu");

        ClientTlsFactorySupport.resolveClientTlsFactory(conf, executor, executor, null);

        assertThat(ByNameCapturingTlsFactory.capturedParams)
                .as("comma-separated key=value tlsFactoryConfig is parsed into the factory init params")
                .containsEntry("issuerUrl", "https://idp.example")
                .containsEntry("region", "eu");
    }

    @Test
    public void byNameFactoryFailsLoudWhenClassNotInstantiable() {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl("pulsar+ssl://localhost:6651");
        conf.setTlsFactoryClassName("com.example.DoesNotExist");

        assertThatThrownBy(() -> ClientTlsFactorySupport.resolveClientTlsFactory(conf, executor, executor, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tlsFactoryClassName");
    }

    // PIP-478 FIX G: the by-name reflective instantiation must honor the thread context classloader
    // (PulsarAdminImpl sets it to the plugin loader), not only the defining loader. A child-first TCCL loads
    // its own copy of the factory class, so the instance's classloader reveals which path was taken: with the
    // fix it is the TCCL; the pre-fix Class.forName(name) would have returned the app/defining loader's copy.
    @Test
    public void byNameFactoryHonorsThreadContextClassLoader() throws Exception {
        String name = TcclOnlyFactory.class.getName();
        TcclFactoryClassLoader tccl = new TcclFactoryClassLoader(getClass().getClassLoader(), name);
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(tccl);
            PulsarTlsFactory factory =
                    ClientTlsFactorySupport.instantiateNamedFactory(name, "tlsFactoryClassName");

            assertThat(factory.getClass().getClassLoader())
                    .as("factory loaded through the thread context classloader, not the defining loader")
                    .isSameAs(tccl);
            assertThat((Object) factory.getClass())
                    .as("a distinct Class from the one the defining loader resolves")
                    .isNotSameAs(TcclOnlyFactory.class);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    /**
     * A child-first {@link ClassLoader} that defines its own copy of a single target class (delegating every
     * other class to the parent), so a class loaded through it is a distinct {@link Class} from the parent's.
     */
    private static final class TcclFactoryClassLoader extends ClassLoader {
        private final String targetName;

        TcclFactoryClassLoader(ClassLoader parent, String targetName) {
            super(parent);
            this.targetName = targetName;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.equals(targetName)) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) {
                    String path = name.replace('.', '/') + ".class";
                    try (InputStream in = getParent().getResourceAsStream(path)) {
                        if (in == null) {
                            throw new ClassNotFoundException(name);
                        }
                        byte[] bytes = in.readAllBytes();
                        loaded = defineClass(name, bytes, 0, bytes.length);
                    } catch (IOException e) {
                        throw new ClassNotFoundException(name, e);
                    }
                }
                if (resolve) {
                    resolveClass(loaded);
                }
                return loaded;
            }
        }
    }

    /** A public no-arg {@link PulsarTlsFactory} used only to observe which classloader instantiated it. */
    public static final class TcclOnlyFactory implements PulsarTlsFactory {
        public TcclOnlyFactory() {
        }

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
                Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public void close() {
        }
    }

    /**
     * A public no-arg {@link PulsarTlsFactory} for the by-name reflective-instantiation test; records the
     * init-context params statically (the factory is created reflectively, so no instance reference is held).
     */
    public static final class ByNameCapturingTlsFactory implements PulsarTlsFactory {
        static volatile Map<String, String> capturedParams;

        public ByNameCapturingTlsFactory() {
        }

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            capturedParams = context.params();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        public void close() {
        }
    }

    /** A minimal {@link PulsarTlsFactory} that records the init-context params and serves an empty client context. */
    private static final class ParamsCapturingTlsFactory implements PulsarTlsFactory {
        private volatile Map<String, String> capturedParams;

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            this.capturedParams = context.params();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass) {
            if (instanceClass != SslContext.class) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            try {
                SslContext context = SslContextBuilder.forClient().build();
                TlsHandle<T> handle = new TlsHandle<>() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public T get() {
                        return (T) context;
                    }

                    @Override
                    public void dispose() {
                    }
                };
                return CompletableFuture.completedFuture(Optional.of(handle));
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass, Consumer<T> onLoadOrReload) {
            return createInstance(purpose, instanceClass).thenApply(opt -> {
                opt.ifPresent(handle -> onLoadOrReload.accept(handle.get()));
                return opt;
            });
        }

        @Override
        public void close() {
        }
    }
}
