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
package org.apache.pulsar.testclient;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.apache.pulsar.client.admin.internal.PulsarAdminBuilderImpl;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.tls.TlsPolicy;
import org.testng.Assert;
import org.testng.annotations.Test;

public class PerfClientUtilsTest {

    public static class MyAuth implements Authentication {
        @Override
        public String getAuthMethodName() {
            return null;
        }

        @SuppressWarnings("deprecation")
        @Override
        public void configure(Map<String, String> authParams) {
        }

        @Override
        public void start() throws PulsarClientException {
        }

        @Override
        public void close() throws IOException {
        }
    }

    @Test
    public void hostnameVerificationAloneDoesNotEnableTls() {
        // conf/client.conf ships tlsEnableHostnameVerification=true since 5.0 (PIP-478), and picocli
        // resolves it through descriptionKey, so every pulsar-perf invocation in a distribution sees
        // TRUE here. Treating that as intent wires a TlsPolicy, which flips useTls on the V5 builder and
        // makes the client attempt a TLS handshake against a plaintext pulsar:// endpoint.
        final PerformanceBaseArguments plaintext = new PerformanceArgumentsTestDefault("");
        plaintext.serviceURL = "pulsar://my-pulsar:6650";
        plaintext.tlsTrustCertsFilePath = "";
        plaintext.tlsHostnameVerificationEnable = true;
        Assert.assertFalse(PerfClientUtils.wantsTls(plaintext));

        // The genuine signals still enable it.
        final PerformanceBaseArguments byUrl = new PerformanceArgumentsTestDefault("");
        byUrl.serviceURL = "pulsar+ssl://my-pulsar:6651";
        byUrl.tlsTrustCertsFilePath = "";
        Assert.assertTrue(PerfClientUtils.wantsTls(byUrl));

        final PerformanceBaseArguments byTrustPath = new PerformanceArgumentsTestDefault("");
        byTrustPath.serviceURL = "pulsar://my-pulsar:6650";
        byTrustPath.tlsTrustCertsFilePath = "/tls/ca.pem";
        Assert.assertTrue(PerfClientUtils.wantsTls(byTrustPath));

        final PerformanceBaseArguments byAllowInsecure = new PerformanceArgumentsTestDefault("");
        byAllowInsecure.serviceURL = "pulsar://my-pulsar:6650";
        byAllowInsecure.tlsTrustCertsFilePath = "";
        byAllowInsecure.tlsAllowInsecureConnection = true;
        Assert.assertTrue(PerfClientUtils.wantsTls(byAllowInsecure));
    }

    @Test
    public void testClientCreation() throws Exception {

        final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");

        args.tlsHostnameVerificationEnable = true;
        args.authPluginClassName = MyAuth.class.getName();
        args.authParams = "params";
        args.enableBusyWait = true;
        args.maxConnections = 100;
        args.ioThreads = 16;
        args.listenerName = "listener";
        args.listenerThreads = 12;
        args.statsIntervalSeconds = Long.MAX_VALUE;
        args.serviceURL = "pulsar+ssl://my-pulsar:6651";
        args.tlsTrustCertsFilePath = "path";
        args.tlsAllowInsecureConnection = true;
        args.maxLookupRequest = 100000;
        args.memoryLimit = 10240;

        final ClientBuilderImpl builder = (ClientBuilderImpl) PerfClientUtils.createClientBuilderFromArguments(args);
        final ClientConfigurationData conf = builder.getClientConfigurationData();

        Assert.assertTrue(conf.isTlsHostnameVerificationEnable());
        Assert.assertEquals(conf.getAuthPluginClassName(), MyAuth.class.getName());
        Assert.assertEquals(conf.getAuthParams(), "params");
        Assert.assertTrue(conf.isEnableBusyWait());
        Assert.assertEquals(conf.getConnectionsPerBroker(), 100);
        Assert.assertEquals(conf.getNumIoThreads(), 16);
        Assert.assertEquals(conf.getListenerName(), "listener");
        Assert.assertEquals(conf.getNumListenerThreads(), 12);
        Assert.assertEquals(conf.getStatsIntervalSeconds(), Long.MAX_VALUE);
        Assert.assertEquals(conf.getServiceUrl(), "pulsar+ssl://my-pulsar:6651");
        Assert.assertEquals(conf.getTlsTrustCertsFilePath(), "path");
        Assert.assertTrue(conf.isTlsAllowInsecureConnection());
        Assert.assertEquals(conf.getMaxLookupRequest(), 100000);
        Assert.assertNull(conf.getProxyServiceUrl());
        Assert.assertNull(conf.getProxyProtocol());
        Assert.assertEquals(conf.getMemoryLimitBytes(), 10240L);

    }

    /**
     * PIP-478: the admin leg must be pinned on the same two axes as the binary leg, otherwise the HTTPS admin
     * calls parse the broker certificate through the JVM provider search order while the data connection is
     * pinned — a FIPS-shaped run on the very tool whose flags exist to validate a pinned cluster.
     */
    @Test
    public void adminBuilderCarriesBothProviderAxes() throws Exception {
        final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");
        args.serviceURL = "pulsar+ssl://my-pulsar:6651";
        args.jsseProvider = "BCJSSE";
        args.jcaProvider = "BCFIPS";

        final PulsarAdminBuilderImpl builder = (PulsarAdminBuilderImpl) PerfClientUtils
                .createAdminBuilderFromArguments(args, "https://my-pulsar:8443");

        assertThat(builder.getConf().getJsseProvider()).isEqualTo("BCJSSE");
        assertThat(builder.getConf().getJcaProvider()).isEqualTo("BCFIPS");
    }

    @Test
    public void adminBuilderProviderAxesAreUnsetByDefault() throws Exception {
        final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");
        args.serviceURL = "pulsar+ssl://my-pulsar:6651";

        final PulsarAdminBuilderImpl builder = (PulsarAdminBuilderImpl) PerfClientUtils
                .createAdminBuilderFromArguments(args, "https://my-pulsar:8443");

        assertThat(builder.getConf().getJsseProvider()).isNull();
        assertThat(builder.getConf().getJcaProvider()).isNull();
    }

    @Test
    public void testClientCreationWithKeyStoreEnabledViaConfFile() throws Exception {
        Path testConf = Files.createTempFile("keystore-test", ".conf");
        try {
            Files.writeString(testConf, "brokerServiceUrl=pulsar+ssl://pulsar:6651\n"
                    + "useKeyStoreTls=true\n"
                    + String.format("authPlugin=%s\n", MyAuth.class.getName())
                    + "tlsTrustStoreType=PKCS12\n"
                    + "tlsTrustStorePath=./tlsTrustStorePath\n"
                    + "tlsTrustStorePassword=tlsTrustStorePassword\n"
                    + "tlsKeyStorePath=./tlsKeyStorePath\n"
                    + "tlsKeyStorePassword=tlsKeyStorePassword\n"
                    + "tlsKeyStoreType=JKS\n");
            final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");
            Properties prop = new Properties(System.getProperties());
            try (FileInputStream fis = new FileInputStream(testConf.toString())) {
                prop.load(fis);
            }
            args.getCommander().setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));
            args.parse(new String[]{});
            final ClientBuilderImpl builder =
                    (ClientBuilderImpl) PerfClientUtils.createClientBuilderFromArguments(args);
            final ClientConfigurationData conf = builder.getClientConfigurationData();

            Assert.assertEquals(conf.getServiceUrl(), "pulsar+ssl://pulsar:6651");
            Assert.assertEquals(conf.getAuthentication().getClass().getName(), MyAuth.class.getName());
            Assert.assertTrue(conf.isUseKeyStoreTls());
            Assert.assertEquals(conf.getTlsTrustStoreType(), "PKCS12");
            Assert.assertEquals(conf.getTlsTrustStorePath(), "./tlsTrustStorePath");
            Assert.assertEquals(conf.getTlsTrustStorePassword(), "tlsTrustStorePassword");
            Assert.assertEquals(conf.getTlsKeyStoreType(), "JKS");
            Assert.assertEquals(conf.getTlsKeyStorePath(), "./tlsKeyStorePath");
            Assert.assertEquals(conf.getTlsKeyStorePassword(), "tlsKeyStorePassword");
        } finally {
            Files.deleteIfExists(testConf);
        }
    }

    @Test
    public void testClientCreationWithKeyStoreEnabled() throws Exception {

        final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");

        args.authPluginClassName = MyAuth.class.getName();
        args.authParams = "params";
        args.serviceURL = "pulsar+ssl://my-pulsar:6651";
        args.useKeyStoreTls = true;
        args.tlsTrustStoreType = "PKCS12";
        args.tlsTrustStorePath = "./tlsTrustStorePath";
        args.tlsTrustStorePassword = "tlsTrustStorePassword";
        args.tlsKeyStoreType = "JKS";
        args.tlsKeyStorePath = "./tlsKeyStorePath";
        args.tlsKeyStorePassword = "tlsKeyStorePassword";

        final ClientBuilderImpl builder = (ClientBuilderImpl) PerfClientUtils.createClientBuilderFromArguments(args);
        final ClientConfigurationData conf = builder.getClientConfigurationData();

        Assert.assertEquals(conf.getAuthPluginClassName(), MyAuth.class.getName());
        Assert.assertEquals(conf.getAuthParams(), "params");
        Assert.assertEquals(conf.getServiceUrl(), "pulsar+ssl://my-pulsar:6651");
        Assert.assertTrue(conf.isUseKeyStoreTls());
        Assert.assertEquals(conf.getTlsTrustStoreType(), "PKCS12");
        Assert.assertEquals(conf.getTlsTrustStorePath(), "./tlsTrustStorePath");
        Assert.assertEquals(conf.getTlsTrustStorePassword(), "tlsTrustStorePassword");
        Assert.assertEquals(conf.getTlsKeyStoreType(), "JKS");
        Assert.assertEquals(conf.getTlsKeyStorePath(), "./tlsKeyStorePath");
        Assert.assertEquals(conf.getTlsKeyStorePassword(), "tlsKeyStorePassword");

    }

    @Test
    public void testAdminCreationWithKeyStoreEnabled() throws Exception {

        final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");

        args.authPluginClassName = MyAuth.class.getName();
        args.authParams = "params";
        args.useKeyStoreTls = true;
        args.tlsTrustStoreType = "PKCS12";
        args.tlsTrustStorePath = "./tlsTrustStorePath";
        args.tlsTrustStorePassword = "tlsTrustStorePassword";
        args.tlsKeyStoreType = "JKS";
        args.tlsKeyStorePath = "./tlsKeyStorePath";
        args.tlsKeyStorePassword = "tlsKeyStorePassword";

        final PulsarAdminBuilderImpl builder = (PulsarAdminBuilderImpl) PerfClientUtils
                .createAdminBuilderFromArguments(args, "https://localhost:8081");
        final ClientConfigurationData conf = builder.getConf();

        Assert.assertEquals(conf.getServiceUrl(), "https://localhost:8081");
        Assert.assertEquals(conf.getAuthentication().getClass().getName(), MyAuth.class.getName());
        Assert.assertTrue(conf.isUseKeyStoreTls());
        Assert.assertEquals(conf.getTlsTrustStoreType(), "PKCS12");
        Assert.assertEquals(conf.getTlsTrustStorePath(), "./tlsTrustStorePath");
        Assert.assertEquals(conf.getTlsTrustStorePassword(), "tlsTrustStorePassword");
        Assert.assertEquals(conf.getTlsKeyStoreType(), "JKS");
        Assert.assertEquals(conf.getTlsKeyStorePath(), "./tlsKeyStorePath");
        Assert.assertEquals(conf.getTlsKeyStorePassword(), "tlsKeyStorePassword");

    }

    @Test
    public void testAdminCreationWithKeyStoreEnabledViaConfFile() throws Exception {
        Path testConf = Files.createTempFile("keystore-test", ".conf");
        try {
            Files.writeString(testConf, "brokerServiceUrl=pulsar+ssl://pulsar:6651\n"
                    + "useKeyStoreTls=true\n"
                    + String.format("authPlugin=%s\n", MyAuth.class.getName())
                    + "tlsTrustStoreType=PKCS12\n"
                    + "tlsTrustStorePath=./tlsTrustStorePath\n"
                    + "tlsTrustStorePassword=tlsTrustStorePassword\n"
                    + "tlsKeyStorePath=./tlsKeyStorePath\n"
                    + "tlsKeyStorePassword=tlsKeyStorePassword\n"
                    + "tlsKeyStoreType=JKS\n");
            final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");
            Properties prop = new Properties(System.getProperties());
            try (FileInputStream fis = new FileInputStream(testConf.toString())) {
                prop.load(fis);
            }
            args.getCommander().setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));
            args.parse(new String[]{});
            final PulsarAdminBuilderImpl builder =
                    (PulsarAdminBuilderImpl) PerfClientUtils.createAdminBuilderFromArguments(args,
                            "https://localhost:8081");
            final ClientConfigurationData conf = builder.getConf();

            Assert.assertEquals(conf.getServiceUrl(), "https://localhost:8081");
            Assert.assertEquals(conf.getAuthentication().getClass().getName(), MyAuth.class.getName());
            Assert.assertTrue(conf.isUseKeyStoreTls());
            Assert.assertEquals(conf.getTlsTrustStoreType(), "PKCS12");
            Assert.assertEquals(conf.getTlsTrustStorePath(), "./tlsTrustStorePath");
            Assert.assertEquals(conf.getTlsTrustStorePassword(), "tlsTrustStorePassword");
            Assert.assertEquals(conf.getTlsKeyStoreType(), "JKS");
            Assert.assertEquals(conf.getTlsKeyStorePath(), "./tlsKeyStorePath");
            Assert.assertEquals(conf.getTlsKeyStorePassword(), "tlsKeyStorePassword");
        } finally {
            Files.deleteIfExists(testConf);
        }
    }

    @Test
    public void v5TlsPolicyCarriesKeyStoreMaterial() {
        final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");
        args.serviceURL = "pulsar://my-pulsar:6650";
        args.useKeyStoreTls = true;
        args.tlsTrustStoreType = "PKCS12";
        args.tlsTrustStorePath = "./tlsTrustStorePath";
        args.tlsTrustStorePassword = "tlsTrustStorePassword";
        args.tlsKeyStoreType = "JKS";
        args.tlsKeyStorePath = "./tlsKeyStorePath";
        args.tlsKeyStorePassword = "tlsKeyStorePassword";
        args.tlsHostnameVerificationEnable = true;

        // --use-keystore-tls is an explicit TLS intent, like --trust-cert-file.
        assertThat(PerfClientUtils.wantsTls(args)).isTrue();

        final TlsPolicy policy = PerfClientUtils.createV5TlsPolicy(args);

        assertThat(policy.format()).isEqualTo(TlsPolicy.Format.KEYSTORE);
        assertThat(policy.trustStoreType()).isEqualTo("PKCS12");
        assertThat(policy.trustStorePath()).isEqualTo("./tlsTrustStorePath");
        assertThat(policy.trustStorePassword()).isEqualTo("tlsTrustStorePassword");
        assertThat(policy.keyStoreType()).isEqualTo("JKS");
        assertThat(policy.keyStorePath()).isEqualTo("./tlsKeyStorePath");
        assertThat(policy.keyStorePassword()).isEqualTo("tlsKeyStorePassword");
        assertThat(policy.trustCertsFilePath()).isNull();
        assertThat(policy.enableHostnameVerification()).isTrue();
    }

    @Test
    public void v5TlsPolicyKeyStoreWithoutClientKeyStore() {
        final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");
        args.useKeyStoreTls = true;
        args.tlsTrustStorePath = "./tlsTrustStorePath";

        final TlsPolicy policy = PerfClientUtils.createV5TlsPolicy(args);

        assertThat(policy.format()).isEqualTo(TlsPolicy.Format.KEYSTORE);
        assertThat(policy.trustStorePath()).isEqualTo("./tlsTrustStorePath");
        assertThat(policy.trustStorePassword()).isNull();
        assertThat(policy.keyStorePath()).isNull();
        assertThat(policy.keyStorePassword()).isNull();
    }

    @Test
    public void v5TlsPolicyIsPemWithoutKeyStoreTls() {
        final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");
        args.serviceURL = "pulsar+ssl://my-pulsar:6651";
        args.tlsTrustCertsFilePath = "/tls/ca.pem";

        final TlsPolicy policy = PerfClientUtils.createV5TlsPolicy(args);

        assertThat(policy.format()).isEqualTo(TlsPolicy.Format.PEM);
        assertThat(policy.trustCertsFilePath()).isEqualTo("/tls/ca.pem");
        assertThat(policy.trustStorePath()).isNull();
        assertThat(policy.keyStorePath()).isNull();
    }

    @Test
    public void v5TlsPolicyKeyStoreTlsIgnoresTrustCertFile() {
        final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");
        args.useKeyStoreTls = true;
        args.tlsTrustStorePath = "./tlsTrustStorePath";
        args.tlsTrustCertsFilePath = "/tls/ca.pem";

        // Same as the v4 client: keystore TLS wins over a PEM trust file, e.g. one left in client.conf.
        final TlsPolicy policy = PerfClientUtils.createV5TlsPolicy(args);

        assertThat(policy.format()).isEqualTo(TlsPolicy.Format.KEYSTORE);
        assertThat(policy.trustStorePath()).isEqualTo("./tlsTrustStorePath");
        assertThat(policy.trustCertsFilePath()).isNull();
    }

    @Test
    public void testClientCreationWithProxy() throws Exception {

        final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");

        args.serviceURL = "pulsar+ssl://my-pulsar:6651";
        args.proxyServiceURL = "pulsar+ssl://my-proxy-pulsar:4443";
        args.proxyProtocol = ProxyProtocol.SNI;

        final ClientBuilderImpl builder = (ClientBuilderImpl) PerfClientUtils.createClientBuilderFromArguments(args);
        final ClientConfigurationData conf = builder.getClientConfigurationData();

        Assert.assertEquals(conf.getProxyServiceUrl(), "pulsar+ssl://my-proxy-pulsar:4443");
        Assert.assertEquals(conf.getProxyProtocol(), ProxyProtocol.SNI);

    }

    @Test
    public void testClientCreationWithProxyDefinedInConfFile() throws Exception {

        Path testConf = Files.createTempFile("test", ".conf");
        try {
            Files.writeString(testConf, "brokerServiceUrl=pulsar+ssl://my-pulsar:6651\n"
                    + "proxyServiceUrl=pulsar+ssl://my-proxy-pulsar:4443\n"
                    + "proxyProtocol=SNI");

            final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");
            Properties prop = new Properties(System.getProperties());
            try (FileInputStream fis = new FileInputStream(testConf.toString())) {
                prop.load(fis);
            }
            args.getCommander().setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));
            args.parse(new String[]{});
            final ClientBuilderImpl builder =
                    (ClientBuilderImpl) PerfClientUtils.createClientBuilderFromArguments(args);
            final ClientConfigurationData conf = builder.getClientConfigurationData();

            Assert.assertEquals(conf.getProxyServiceUrl(), "pulsar+ssl://my-proxy-pulsar:4443");
            Assert.assertEquals(conf.getProxyProtocol(), ProxyProtocol.SNI);
        } finally {
            Files.deleteIfExists(testConf);
        }
    }

    @Test
    public void testClientCreationWithEmptyProxyPropertyInConfFile() throws Exception {

        Path testConf = Files.createTempFile("test", ".conf");
        try {
            Files.writeString(testConf, "brokerServiceUrl=pulsar+ssl://my-pulsar:6651\n"
                    + "proxyServiceUrl=\n"
                    + "proxyProtocol=");

            final PerformanceBaseArguments args = new PerformanceArgumentsTestDefault("");
            Properties prop = new Properties(System.getProperties());
            try (FileInputStream fis = new FileInputStream(testConf.toString())) {
                prop.load(fis);
            }
            args.getCommander().setDefaultValueProvider(PulsarPerfTestPropertiesProvider.create(prop));
            args.parse(new String[]{});

            final ClientBuilderImpl builder =
                    (ClientBuilderImpl) PerfClientUtils.createClientBuilderFromArguments(args);
            final ClientConfigurationData conf = builder.getClientConfigurationData();

            Assert.assertEquals(conf.getProxyServiceUrl(), "");
            Assert.assertNull(conf.getProxyProtocol());
        } finally {
            Files.deleteIfExists(testConf);
        }
    }

    /**
     * Each perf command holds several latency recorders (a live and a cumulative one per measured latency).
     * HdrHistogram grows the counts array by roughly 10x per significant digit, and at 5 digits a single
     * recorder over these same ranges costs 14-16 MB. Pin the bound so raising the precision again fails
     * here instead of silently costing hundreds of megabytes per run.
     */
    @Test
    public void latencyHistogramsStaySmallAtTheConfiguredPrecision() {
        long[] rangesUsedByPerfClients = {
                TimeUnit.HOURS.toMicros(1), // publish / ack / managed-ledger write latency, in microseconds
                TimeUnit.DAYS.toMillis(10), // end-to-end consume / read latency, in milliseconds
        };
        for (long range : rangesUsedByPerfClients) {
            Histogram histogram = new Histogram(range, PerfClientUtils.LATENCY_HISTOGRAM_SIGNIFICANT_DIGITS);
            assertThat(histogram.getEstimatedFootprintInBytes())
                    .as("histogram footprint for range %d", range)
                    .isLessThan(512 * 1024);
        }
    }
}

class PerformanceArgumentsTestDefault extends PerformanceBaseArguments {
    public PerformanceArgumentsTestDefault(String cmdName) {
        super(cmdName);
    }


    @Override
    public void run() throws Exception {

    }
}
