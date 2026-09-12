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

import static org.apache.pulsar.common.tls.impl.TlsReloadMetrics.LAST_RELOAD_SUCCESS_GAUGE;
import static org.apache.pulsar.common.tls.impl.TlsReloadMetrics.RELOAD_COUNTER;
import static org.apache.pulsar.common.tls.impl.TlsTestSupport.resource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.handler.ssl.SslContext;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.io.FileUtils;
import org.apache.pulsar.tls.TlsPolicy;
import org.apache.pulsar.tls.TlsPurpose;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TlsReloadMetricsTest {

    private static final String RSA_CA = resource("certificate-authority/certs/ca.cert.pem");
    private static final String BROKER_CERT = resource("certificate-authority/server-keys/broker.cert.pem");
    private static final String BROKER_KEY = resource("certificate-authority/server-keys/broker.key-pk8.pem");
    private static final String PROXY_CERT = resource("certificate-authority/server-keys/proxy.cert.pem");
    private static final String PROXY_KEY = resource("certificate-authority/server-keys/proxy.key-pk8.pem");

    private static final AttributeKey<String> PURPOSE = AttributeKey.stringKey("purpose");
    private static final AttributeKey<String> RESULT = AttributeKey.stringKey("result");

    private ScheduledExecutorService scheduler;
    private final Executor directExecutor = Runnable::run;
    private InMemoryMetricReader metricReader;
    private OpenTelemetry openTelemetry;
    private Path tempDir;

    @BeforeMethod
    public void setUp() throws Exception {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        metricReader = InMemoryMetricReader.create();
        openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metricReader).build())
                .build();
        tempDir = Files.createTempDirectory("pip478-tls-metrics-");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        scheduler.shutdownNow();
        if (tempDir != null) {
            FileUtils.deleteDirectory(tempDir.toFile());
        }
    }

    private FileBasedTlsFactory factory(Map<TlsPurpose, TlsPolicy> policies, FileBasedTlsFactorySettings settings) {
        FileBasedTlsFactory factory = new FileBasedTlsFactory(policies, settings);
        factory.initialize(TlsTestSupport.initContext(scheduler, directExecutor, openTelemetry)).join();
        return factory;
    }

    @Test
    public void initialLoadRecordsSuccessCounterAndGauge() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());

        factory.createInstance(TlsPurpose.BROKER, SslContext.class).join().get().dispose();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        assertThat(counter(metrics, "broker", "success")).isEqualTo(1L);
        assertThat(counter(metrics, "broker", "failure")).isEqualTo(0L);
        assertThat(gauge(metrics, "broker")).as("last_reload_success gauge is set on success").isPositive();
        factory.close();
    }

    @Test
    public void initialLoadFailureRecordsFailureCounterAndNoGauge() {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA,
                        tempDir.resolve("missing-cert.pem").toString(),
                        tempDir.resolve("missing-key.pem").toString())),
                FileBasedTlsFactorySettings.defaults());

        assertThatThrownBy(() -> factory.createInstance(TlsPurpose.BROKER, SslContext.class).join())
                .isInstanceOf(Exception.class);

        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        assertThat(counter(metrics, "broker", "failure")).isEqualTo(1L);
        assertThat(counter(metrics, "broker", "success")).isEqualTo(0L);
        assertThat(gauge(metrics, "broker")).as("no last_reload_success without a successful load").isEqualTo(-1L);
        factory.close();
    }

    @Test
    public void rotationRecordsSecondSuccessfulReload() throws Exception {
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());

        factory.createInstance(TlsPurpose.BROKER, SslContext.class, new CopyOnWriteArrayList<SslContext>()::add).join();
        assertThat(counter(metricReader.collectAllMetrics(), "broker", "success")).isEqualTo(1L);

        overwriteServerCerts(PROXY_CERT, PROXY_KEY);
        // The rotation poll delivers a rebuilt instance and records a second successful reload.
        Awaitility.await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(counter(metricReader.collectAllMetrics(), "broker", "success"))
                        .isEqualTo(2L));
        assertThat(counter(metricReader.collectAllMetrics(), "broker", "failure")).isEqualTo(0L);
        factory.close();
    }

    /**
     * A repeat one-shot acquisition with unchanged files is not a reload: on the acquire-per-connection paths
     * every new connection calls {@code createInstance}, and counting those would inflate the reload counter and
     * keep advancing {@code last_reload_success}, masking a rotation that has silently stalled.
     */
    @Test
    public void repeatedOneShotWithUnchangedFilesIsNotCountedAsReload() throws Exception {
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        MutableClock clock = new MutableClock(Instant.parse("2026-07-20T10:00:00Z"));
        FileBasedTlsFactory factory = new FileBasedTlsFactory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.defaults());
        factory.initialize(TlsTestSupport.initContext(scheduler, directExecutor, openTelemetry, clock)).join();

        factory.createInstance(TlsPurpose.BROKER, SslContext.class).join().get().dispose();
        long firstGauge = gauge(metricReader.collectAllMetrics(), "broker");
        assertThat(counter(metricReader.collectAllMetrics(), "broker", "success")).isEqualTo(1L);
        assertThat(firstGauge).isEqualTo(clock.instant().getEpochSecond());

        clock.advanceSeconds(3600);
        factory.createInstance(TlsPurpose.BROKER, SslContext.class).join().get().dispose();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        assertThat(counter(metrics, "broker", "success"))
                .as("a cached re-acquisition is not a (re)load").isEqualTo(1L);
        assertThat(counter(metrics, "broker", "failure")).isEqualTo(0L);
        assertThat(gauge(metrics, "broker"))
                .as("last_reload_success must not advance without a real reload").isEqualTo(firstGauge);

        // A real rotation still counts, on the same one-shot path.
        overwriteServerCerts(PROXY_CERT, PROXY_KEY);
        factory.createInstance(TlsPurpose.BROKER, SslContext.class).join().get().dispose();
        metrics = metricReader.collectAllMetrics();
        assertThat(counter(metrics, "broker", "success")).isEqualTo(2L);
        assertThat(gauge(metrics, "broker")).isEqualTo(clock.instant().getEpochSecond());
        factory.close();
    }

    /** A test clock whose instant the test advances explicitly, so gauge movement is asserted exactly. */
    private static final class MutableClock extends Clock {
        private volatile Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        private void advanceSeconds(long seconds) {
            now = now.plusSeconds(seconds);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    @Test
    public void failedRotationRecordsReloadFailure() throws Exception {
        TlsPolicy policy = copyServerCertsToTemp(BROKER_CERT, BROKER_KEY);
        FileBasedTlsFactory factory = factory(Map.of(TlsPurpose.BROKER, policy),
                FileBasedTlsFactorySettings.builder().refreshIntervalSeconds(1).build());

        factory.createInstance(TlsPurpose.BROKER, SslContext.class, new CopyOnWriteArrayList<SslContext>()::add).join();
        assertThat(counter(metricReader.collectAllMetrics(), "broker", "success")).isEqualTo(1L);

        // Corrupt the cert file (mtime advanced so the change is observed): the rebuild fails and is counted.
        Files.writeString(tempDir.resolve("cert.pem"), "-----BEGIN CERTIFICATE-----\nnot a cert\n");
        Files.setLastModifiedTime(tempDir.resolve("cert.pem"), FileTime.fromMillis(System.currentTimeMillis() + 5000));
        Awaitility.await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(counter(metricReader.collectAllMetrics(), "broker", "failure"))
                        .isEqualTo(1L));
        factory.close();
    }

    /**
     * The emitted instrument names are the flat {@code pulsar.tls.*} names, under the shared TLS
     * instrumentation scope, and the varying dimensions are attributes. Asserted on the metrics that
     * actually reach the reader, not on a name helper.
     */
    @Test
    public void emitsFlatInstrumentNamesUnderTheSharedScope() throws Exception {
        FileBasedTlsFactory factory = factory(
                Map.of(TlsPurpose.BROKER, TlsPolicy.pem(RSA_CA, BROKER_CERT, BROKER_KEY)),
                FileBasedTlsFactorySettings.defaults());

        factory.createInstance(TlsPurpose.BROKER, SslContext.class).join().get().dispose();

        Collection<MetricData> metrics = metricReader.collectAllMetrics();
        assertThat(metrics).extracting(MetricData::getName)
                .containsExactlyInAnyOrder("pulsar.tls.reload", "pulsar.tls.last_reload_success");
        assertThat(metrics).allSatisfy(m -> assertThat(m.getInstrumentationScopeInfo().getName())
                .isEqualTo(TlsReloadMetrics.INSTRUMENTATION_SCOPE_NAME));
        assertThat(counter(metrics, "broker", "success")).isEqualTo(1L);
        assertThat(gauge(metrics, "broker")).isPositive();
        factory.close();
    }

    /** The reload counter value for a purpose+result, or 0 when the point is absent. */
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

    /** The last-reload-success gauge (unix seconds) for a purpose, or -1 when the point is absent. */
    private long gauge(Collection<MetricData> metrics, String purpose) {
        return metrics.stream()
                .filter(m -> m.getName().equals(LAST_RELOAD_SUCCESS_GAUGE))
                .flatMap(m -> m.getLongGaugeData().getPoints().stream())
                .filter(p -> purpose.equals(p.getAttributes().get(PURPOSE)))
                .mapToLong(LongPointData::getValue)
                .findFirst()
                .orElse(-1L);
    }

    private TlsPolicy copyServerCertsToTemp(String certSrc, String keySrc) throws Exception {
        Files.copy(Paths.get(RSA_CA), tempDir.resolve("ca.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(certSrc), tempDir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(keySrc), tempDir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
        return TlsPolicy.pem(tempDir.resolve("ca.pem").toString(),
                tempDir.resolve("cert.pem").toString(), tempDir.resolve("key.pem").toString());
    }

    private void overwriteServerCerts(String certSrc, String keySrc) throws Exception {
        Files.copy(Paths.get(certSrc), tempDir.resolve("cert.pem"), StandardCopyOption.REPLACE_EXISTING);
        Files.copy(Paths.get(keySrc), tempDir.resolve("key.pem"), StandardCopyOption.REPLACE_EXISTING);
        long later = System.currentTimeMillis() + 5000;
        Files.setLastModifiedTime(tempDir.resolve("cert.pem"), FileTime.fromMillis(later));
        Files.setLastModifiedTime(tempDir.resolve("key.pem"), FileTime.fromMillis(later));
    }
}
