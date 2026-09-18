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

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.io.Resources;
import io.netty.handler.ssl.SslContext;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.broker.tls.TlsFactorySupport;
import org.apache.pulsar.proxy.stats.PulsarProxyOpenTelemetry;
import org.apache.pulsar.tls.PulsarTlsFactory;
import org.apache.pulsar.tls.TlsPurpose;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * PIP-478 stage 4b: the proxy threads its OpenTelemetry root into the PROXY-purpose
 * {@code TlsFactoryInitContext}, so the default file-based TLS factory's {@code pulsar.tls.reload} /
 * {@code pulsar.tls.last_reload_success} instruments emit for real instead of being no-ops. The 4a
 * in-memory-reader metrics pattern applied to a server component (the proxy).
 */
public class ProxyTlsFactoryMetricsTest {

    private static final String CA_CERT = resource("certificate-authority/certs/ca.cert.pem");
    private static final String PROXY_CERT = resource("certificate-authority/server-keys/proxy.cert.pem");
    private static final String PROXY_KEY = resource("certificate-authority/server-keys/proxy.key-pk8.pem");

    private static final String RELOAD_COUNTER = "pulsar.tls.reload";
    private static final String LAST_RELOAD_SUCCESS_GAUGE = "pulsar.tls.last_reload_success";
    private static final AttributeKey<String> PURPOSE = AttributeKey.stringKey("purpose");
    private static final AttributeKey<String> RESULT = AttributeKey.stringKey("result");

    private ScheduledExecutorService scheduler;
    private final Executor directExecutor = Runnable::run;
    private InMemoryMetricReader metricReader;
    private OpenTelemetry openTelemetry;

    @BeforeMethod
    public void setUp() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        metricReader = InMemoryMetricReader.create();
        openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metricReader).build())
                .build();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    public void proxyOpenTelemetryExposesRealRoot() {
        ProxyConfiguration config = new ProxyConfiguration();
        config.setClusterName("test");
        try (PulsarProxyOpenTelemetry proxyOtel = new PulsarProxyOpenTelemetry(config)) {
            assertThat(proxyOtel.getOpenTelemetry())
                    .as("the proxy exposes a real OpenTelemetry root (not OpenTelemetry.noop()) to feed its "
                            + "TlsFactoryInitContext")
                    .isNotNull()
                    .isNotSameAs(OpenTelemetry.noop());
        }
    }

    @Test
    public void proxyServerFactoryEmitsTlsReloadThroughWiredRoot() throws Exception {
        ProxyConfiguration config = new ProxyConfiguration();
        config.setTlsCertificateFilePath(PROXY_CERT);
        config.setTlsKeyFilePath(PROXY_KEY);
        config.setTlsTrustCertsFilePath(CA_CERT);

        // Exactly the proxy's default PROXY-purpose factory, initialized with a real OpenTelemetry root — the
        // wiring ServiceChannelInitializer performs via proxyService.getOpenTelemetry().getOpenTelemetry().
        PulsarTlsFactory factory = ProxyTlsFactories.serverFactory(config, TlsPurpose.PROXY,
                config.getTlsCiphers(), config.getTlsProtocols());
        factory.initialize(TlsFactorySupport.initContext(Map.of(), scheduler, directExecutor, openTelemetry)).join();
        // The initial material load of the PROXY purpose records one successful reload.
        factory.createInstance(TlsPurpose.PROXY, SslContext.class).join().get().dispose();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        // TlsPurpose.PROXY.name() is the lowercase wire label "proxy".
        assertThat(counter(metrics, "proxy", "success"))
                .as("the proxy TLS factory records a successful pulsar.tls.reload through the wired root")
                .isEqualTo(1L);
        assertThat(counter(metrics, "proxy", "failure")).isEqualTo(0L);
        assertThat(gauge(metrics, "proxy")).as("last_reload_success gauge is set on success").isPositive();
        factory.close();
    }

    private long counter(Collection<MetricData> metrics, String purpose, String result) {
        return metrics.stream()
                .filter(m -> m.getName().equals(RELOAD_COUNTER))
                .flatMap(m -> m.getLongSumData().getPoints().stream())
                .filter(p -> purpose.equals(p.getAttributes().get(PURPOSE))
                        && result.equals(p.getAttributes().get(RESULT)))
                .mapToLong(LongPointData::getValue)
                .findFirst()
                .orElse(0L);
    }

    private long gauge(Collection<MetricData> metrics, String purpose) {
        return metrics.stream()
                .filter(m -> m.getName().equals(LAST_RELOAD_SUCCESS_GAUGE))
                .flatMap(m -> m.getLongGaugeData().getPoints().stream())
                .filter(p -> purpose.equals(p.getAttributes().get(PURPOSE)))
                .mapToLong(LongPointData::getValue)
                .findFirst()
                .orElse(-1L);
    }

    private static String resource(String name) {
        return new File(Resources.getResource(name).getPath()).getAbsolutePath();
    }
}
