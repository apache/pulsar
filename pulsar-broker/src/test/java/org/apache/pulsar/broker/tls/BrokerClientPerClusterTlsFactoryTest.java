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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsFactoryInitContext;
import org.apache.pulsar.tls.TlsHandle;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478: per-cluster broker-client TLS factory selection, exercised through the broker rather than through
 * {@code ClientTlsFactorySupport} in isolation. Two things only a broker-level test can show:
 *
 * <ul>
 *   <li><b>{@code wrapBrokerClientPurpose} actually runs.</b> A custom broker-client factory resolves under
 *       {@code BROKER_CLIENT} by the PIP's stable-API contract, but the client transport asks for
 *       {@code CLIENT_DEFAULT}. Without the wrapper a compliant custom factory returns {@code empty()} and the
 *       connection fails. The recording factories below serve <em>only</em> {@code BROKER_CLIENT}, so they
 *       would fail in exactly that way — the previous tests all used {@code "default"}, which never reaches
 *       the wrapper.</li>
 *   <li><b>The class name / config pair resolves atomically.</b> A cluster that names a factory uses its own
 *       config even when blank, so factory A's parameters can never be handed to factory B; the broker-level
 *       config applies only when the cluster names no factory. This is where inverted logic would hide.</li>
 * </ul>
 *
 * <p>The resolved factory is asked for {@code CLIENT_DEFAULT} directly — the same request
 * {@code PulsarChannelInitializer} makes when a connection is established — so no peer cluster has to exist.
 */
@Test(groups = "broker")
public class BrokerClientPerClusterTlsFactoryTest extends MockedPulsarServiceBaseTest {

    private static final String BROKER_LEVEL_CONFIG = "owner=broker-level";
    private static final String CLUSTER_LEVEL_CONFIG = "owner=cluster-level";

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        BrokerLevelTlsFactory.reset();
        ClusterLevelTlsFactory.reset();
        conf.setBrokerClientTlsEnabled(true);
        conf.setBrokerClientTlsFactoryClassName(BrokerLevelTlsFactory.class.getName());
        conf.setBrokerClientTlsFactoryConfig(BROKER_LEVEL_CONFIG);
        internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        internalCleanup();
        BrokerLevelTlsFactory.reset();
        ClusterLevelTlsFactory.reset();
    }

    @Test
    public void theTransportsClientDefaultRequestReachesTheFactoryAsBrokerClient() throws Exception {
        PulsarTlsFactory factory = replicationClientTlsFactory(clusterNaming(null, null));

        // Exactly what PulsarChannelInitializer asks for on the client transport.
        Optional<TlsHandle<SslContext>> handle =
                factory.createInstance(TlsPurpose.CLIENT_DEFAULT, SslContext.class).get(30, TimeUnit.SECONDS);

        // Both this request and the client build's fail-fast probe go through the wrapper, so assert the
        // invariant (every request arrives translated) rather than a request count.
        assertThat(BrokerLevelTlsFactory.REQUESTED_PURPOSES)
                .as("the wrapper must translate the transport's CLIENT_DEFAULT to BROKER_CLIENT before "
                        + "delegating; a compliant BROKER_CLIENT-only custom factory sees only BROKER_CLIENT")
                .isNotEmpty()
                .containsOnly(TlsPurpose.BROKER_CLIENT);
        assertThat(handle).as("so a BROKER_CLIENT-only factory can serve the transport's request").isPresent();
    }

    @Test
    public void aClusterNamingItsOwnFactoryWinsOverTheBrokerLevelOne() throws Exception {
        PulsarTlsFactory factory = replicationClientTlsFactory(
                clusterNaming(ClusterLevelTlsFactory.class.getName(), CLUSTER_LEVEL_CONFIG));

        assertThat(factory.createInstance(TlsPurpose.CLIENT_DEFAULT, SslContext.class).get(30, TimeUnit.SECONDS))
                .isPresent();
        assertThat(ClusterLevelTlsFactory.instantiated)
                .as("the cluster's own factory is the one instantiated").isTrue();
        assertThat(BrokerLevelTlsFactory.instantiated)
                .as("the broker-level factory must not be used by a cluster that names its own").isFalse();
        assertThat(ClusterLevelTlsFactory.initParams)
                .as("and it receives its own cluster-level config")
                .containsExactly(Map.entry("owner", "cluster-level"));
    }

    @Test
    public void aClusterNamingAFactoryWithBlankConfigDoesNotInheritTheBrokerLevelConfig() throws Exception {
        PulsarTlsFactory factory = replicationClientTlsFactory(
                clusterNaming(ClusterLevelTlsFactory.class.getName(), ""));

        assertThat(factory.createInstance(TlsPurpose.CLIENT_DEFAULT, SslContext.class).get(30, TimeUnit.SECONDS))
                .isPresent();
        assertThat(ClusterLevelTlsFactory.initParams)
                .as("the config follows the class name: naming a factory with a blank config means blank, never "
                        + "the broker-level config — otherwise factory A's parameters reach factory B")
                .isEmpty();
    }

    @Test
    public void aClusterNamingNoFactoryInheritsBothBrokerLevelValues() throws Exception {
        PulsarTlsFactory factory = replicationClientTlsFactory(clusterNaming(null, null));

        assertThat(factory.createInstance(TlsPurpose.CLIENT_DEFAULT, SslContext.class).get(30, TimeUnit.SECONDS))
                .isPresent();
        assertThat(BrokerLevelTlsFactory.instantiated)
                .as("the broker-level factory applies when the cluster names none").isTrue();
        assertThat(ClusterLevelTlsFactory.instantiated).isFalse();
        assertThat(BrokerLevelTlsFactory.initParams)
                .as("and the broker-level config applies with it")
                .containsExactly(Map.entry("owner", "broker-level"));
    }

    private ClusterData clusterNaming(String factoryClassName, String factoryConfig) {
        return ClusterData.builder()
                .serviceUrl("http://peer-cluster:8080")
                .brokerServiceUrl("pulsar://peer-cluster:6650")
                .serviceUrlTls("https://peer-cluster:8443")
                .brokerServiceUrlTls("pulsar+ssl://peer-cluster:6651")
                .brokerClientTlsEnabled(true)
                .brokerClientTlsFactoryClassName(factoryClassName)
                .brokerClientTlsFactoryConfig(factoryConfig)
                .build();
    }

    /** The TLS factory the replication (binary) leg to a peer cluster ends up using. */
    private PulsarTlsFactory replicationClientTlsFactory(ClusterData clusterData) {
        PulsarClientImpl client = (PulsarClientImpl) pulsar.getBrokerService()
                .getReplicationClient("peer-cluster", Optional.of(clusterData));
        PulsarTlsFactory factory = client.getConfiguration().getTlsFactory();
        assertThat(factory).as("an opted-in broker-client leg resolves a TLS factory").isNotNull();
        return factory;
    }

    /**
     * A custom broker-client factory that records the purposes it is asked for and the init params it was
     * given. It serves <em>only</em> {@link TlsPurpose#BROKER_CLIENT} — the PIP-478 contract for a custom
     * broker-client factory — so a missing purpose translation shows up as an empty result rather than as a
     * silently passing test.
     */
    abstract static class RecordingTlsFactory implements PulsarTlsFactory {

        abstract void recordPurpose(TlsPurpose purpose);

        abstract void recordParams(Map<String, String> params);

        @Override
        public CompletableFuture<Void> initialize(TlsFactoryInitContext context) {
            recordParams(context.params());
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public <T> CompletableFuture<Optional<TlsHandle<T>>> createInstance(TlsPurpose purpose,
                Class<T> instanceClass) {
            recordPurpose(purpose);
            if (!TlsPurpose.BROKER_CLIENT.equals(purpose) || instanceClass != SslContext.class) {
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

    /** Selected by {@code ServiceConfiguration.brokerClientTlsFactoryClassName}. */
    public static class BrokerLevelTlsFactory extends RecordingTlsFactory {

        static final List<TlsPurpose> REQUESTED_PURPOSES = new CopyOnWriteArrayList<>();
        static volatile Map<String, String> initParams = Map.of();
        static volatile boolean instantiated;

        static void reset() {
            REQUESTED_PURPOSES.clear();
            initParams = Map.of();
            instantiated = false;
        }

        public BrokerLevelTlsFactory() {
            instantiated = true;
        }

        @Override
        void recordPurpose(TlsPurpose purpose) {
            REQUESTED_PURPOSES.add(purpose);
        }

        @Override
        void recordParams(Map<String, String> params) {
            initParams = Map.copyOf(params);
        }
    }

    /** Selected by {@code ClusterData.brokerClientTlsFactoryClassName}, so "which class won" is observable. */
    public static class ClusterLevelTlsFactory extends RecordingTlsFactory {

        static final List<TlsPurpose> REQUESTED_PURPOSES = new CopyOnWriteArrayList<>();
        static volatile Map<String, String> initParams = Map.of();
        static volatile boolean instantiated;

        static void reset() {
            REQUESTED_PURPOSES.clear();
            initParams = Map.of();
            instantiated = false;
        }

        public ClusterLevelTlsFactory() {
            instantiated = true;
        }

        @Override
        void recordPurpose(TlsPurpose purpose) {
            REQUESTED_PURPOSES.add(purpose);
        }

        @Override
        void recordParams(Map<String, String> params) {
            initParams = Map.copyOf(params);
        }
    }
}
