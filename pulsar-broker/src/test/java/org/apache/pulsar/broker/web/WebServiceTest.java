/**
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
package org.apache.pulsar.broker.web;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import lombok.Cleanup;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.MockedBookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.SecurityUtility;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests for the {@code WebService} class. Note that this test only covers the newly added ApiVersionFilter related
 * tests for now as this test class was added quite a bit after the class was written.
 */
@Test(groups = "broker")
public class WebServiceTest {

    private PulsarService pulsar;
    private String BROKER_LOOKUP_URL;
    private String BROKER_LOOKUP_URL_TLS;
    private static final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
    private static final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";
    private static final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/certificate/client.crt";
    private static final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/certificate/client.key";

    /**
     * Test that the {@WebService} class properly passes the allowUnversionedClients value. We do this by setting
     * allowUnversionedClients to true, then making a request with no version, which should go through.
     *
     */
    @Test
    public void testDefaultClientVersion() throws Exception {
        setupEnv(true, "1.0", true, false, false, false, -1, false);
      
        try {
            // Make an HTTP request to lookup a namespace. The request should
            // succeed
            makeHttpRequest(false, false);
        } catch (Exception e) {
            Assert.fail("HTTP request to lookup a namespace shouldn't fail ", e);
        }
    }

    /**
     * Test that if enableTls option is enabled, WebServcie is available both on HTTP and HTTPS.
     *
     * @throws Exception
     */
    @Test
    public void testTlsEnabled() throws Exception {
        setupEnv(false, "1.0", false, true, false, false, -1, false);

        // Make requests both HTTP and HTTPS. The requests should succeed
        try {
            makeHttpRequest(false, false);
        } catch (Exception e) {
            Assert.fail("HTTP request shouldn't fail ", e);
        }
        try {
            makeHttpRequest(true, false);
        } catch (Exception e) {
            Assert.fail("HTTPS request shouldn't fail ", e);
        }
    }

    /**
     * Test that if enableTls option is disabled, WebServcie is available only on HTTP.
     *
     * @throws Exception
     */
    @Test
    public void testTlsDisabled() throws Exception {
        setupEnv(false, "1.0", false, false, false, false, -1, false);

        // Make requests both HTTP and HTTPS. Only the HTTP request should succeed
        try {
            makeHttpRequest(false, false);
        } catch (Exception e) {
            Assert.fail("HTTP request shouldn't fail ", e);
        }
        try {
            makeHttpRequest(true, false);
            Assert.fail("HTTPS request should fail ");
        } catch (Exception e) {
            // Expected
        }
    }

    /**
     * Test that if enableAuth option and allowInsecure option are enabled, WebServcie requires trusted/untrusted client
     * certificate.
     *
     * @throws Exception
     */
    @Test
    public void testTlsAuthAllowInsecure() throws Exception {
        setupEnv(false, "1.0", false, true, true, true, -1, false);

        // Only the request with client certificate should succeed
        try {
            makeHttpRequest(true, false);
            Assert.fail("Request without client certficate should fail");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("HTTP response code: 401"));
        }
        try {
            makeHttpRequest(true, true);
        } catch (Exception e) {
            Assert.fail("Request with client certificate shouldn't fail", e);
        }
    }

    /**
     * Test that if enableAuth option is enabled, WebServcie requires trusted client certificate.
     *
     * @throws Exception
     */
    @Test
    public void testTlsAuthDisallowInsecure() throws Exception {
        setupEnv(false, "1.0", false, true, true, false, -1, false);

        // Only the request with trusted client certificate should succeed
        try {
            makeHttpRequest(true, false);
            Assert.fail("Request without client certficate should fail");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("HTTP response code: 401"));
        }
        try {
            makeHttpRequest(true, true);
        } catch (Exception e) {
            Assert.fail("Request with client certificate shouldn't fail", e);
        }
    }

    @Test
    public void testRateLimiting() throws Exception {
        setupEnv(false, "1.0", false, false, false, false, 10.0, false);

        // Make requests without exceeding the max rate
        for (int i = 0; i < 5; i++) {
            makeHttpRequest(false, false);
            Thread.sleep(200);
        }

        try {
            for (int i = 0; i < 500; i++) {
                makeHttpRequest(false, false);
            }

            fail("Some request should have failed");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("429"));
        }
    }

    @Test
    public void testSplitPath() {
        String result = PulsarWebResource.splitPath("prop/cluster/ns/topic1", 4);
        Assert.assertEquals(result, "topic1");
    }

    @Test
    public void testDisableHttpTraceAndTrackMethods() throws Exception {
        setupEnv(true, "1.0", true, false, false, false, -1, true);

        String url = pulsar.getWebServiceAddress() + "/admin/v2/tenants/my-tenant" + System.currentTimeMillis();

        @Cleanup
        AsyncHttpClient client = new DefaultAsyncHttpClient();

        BoundRequestBuilder builder = client.prepare("TRACE", url);

        Response res = builder.execute().get();

        // This should have failed
        assertEquals(res.getStatusCode(), 405);
        
        builder = client.prepare("TRACK", url);

        res = builder.execute().get();

        // This should have failed
        assertEquals(res.getStatusCode(), 405);
    }

    @Test
    public void testMaxRequestSize() throws Exception {
        setupEnv(true, "1.0", true, false, false, false, -1, false);

        String url = pulsar.getWebServiceAddress() + "/admin/v2/tenants/my-tenant" + System.currentTimeMillis();

        @Cleanup
        AsyncHttpClient client = new DefaultAsyncHttpClient();

        BoundRequestBuilder builder = client.preparePut(url)
                .setHeader("Accept", "application/json")
                .setHeader("Content-Type", "application/json");

        // HTTP server is configured to reject everything > 10K
        TenantInfo info1 = TenantInfo.builder()
                .adminRoles(Collections.singleton(StringUtils.repeat("*", 20 * 1024)))
                .build();
        builder.setBody(ObjectMapperFactory.getThreadLocal().writeValueAsBytes(info1));
        Response res = builder.execute().get();

        // This should have failed
        assertEquals(res.getStatusCode(), 400);

        // Create local cluster
        String localCluster = "test";
        pulsar.getPulsarResources().getClusterResources().createCluster(localCluster, ClusterDataImpl.builder().build());
        TenantInfo info2 = TenantInfo.builder()
                .adminRoles(Collections.singleton(StringUtils.repeat("*", 1 * 1024)))
                .allowedClusters(Sets.newHashSet(localCluster))
                .build();
        builder.setBody(ObjectMapperFactory.getThreadLocal().writeValueAsBytes(info2));

        Response res2 = builder.execute().get();
        assertEquals(res2.getStatusCode(), 204);

        // Simple GET without content size should go through
        Response res3 = client.prepareGet(url)
            .setHeader("Accept", "application/json")
            .setHeader("Content-Type", "application/json")
            .execute()
            .get();
        assertEquals(res3.getStatusCode(), 200);
    }

    @Test
    public void testBrokerReady() throws Exception {
        setupEnv(true, "1.0", true, false, false, false, -1, false);

        String url = pulsar.getWebServiceAddress() + "/admin/v2/brokers/ready";

        @Cleanup
        AsyncHttpClient client = new DefaultAsyncHttpClient();

        Response res = client.prepareGet(url).execute().get();
        assertEquals(res.getStatusCode(), 200);
        assertEquals(res.getResponseBody(), "ok");
    }

    private String makeHttpRequest(boolean useTls, boolean useAuth) throws Exception {
        InputStream response = null;
        try {
            if (useTls) {
                KeyManager[] keyManagers = null;
                if (useAuth) {
                    Certificate[] tlsCert = SecurityUtility.loadCertificatesFromPemFile(TLS_CLIENT_CERT_FILE_PATH);
                    PrivateKey tlsKey = SecurityUtility.loadPrivateKeyFromPemFile(TLS_CLIENT_KEY_FILE_PATH);

                    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
                    ks.load(null, null);
                    ks.setKeyEntry("private", tlsKey, "".toCharArray(), tlsCert);

                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(ks, "".toCharArray());
                    keyManagers = kmf.getKeyManagers();
                }
                TrustManager[] trustManagers = InsecureTrustManagerFactory.INSTANCE.getTrustManagers();
                SSLContext sslCtx = SSLContext.getInstance("TLS");
                sslCtx.init(keyManagers, trustManagers, new SecureRandom());
                HttpsURLConnection.setDefaultSSLSocketFactory(sslCtx.getSocketFactory());
                response = new URL(BROKER_LOOKUP_URL_TLS).openStream();
            } else {
                response = new URL(BROKER_LOOKUP_URL).openStream();
            }
            String resp = CharStreams.toString(new InputStreamReader(response));
            log.info("Response: {}", resp);
            return resp;
        } finally {
            Closeables.close(response, false);
        }
    }

    private void setupEnv(boolean enableFilter, String minApiVersion, boolean allowUnversionedClients,
            boolean enableTls, boolean enableAuth, boolean allowInsecure, double rateLimit, 
            boolean disableTrace) throws Exception {
        if (pulsar != null) {
            throw new Exception("broker already started");
        }
        Set<String> providers = new HashSet<>();
        providers.add("org.apache.pulsar.broker.authentication.AuthenticationProviderTls");

        Set<String> roles = new HashSet<>();
        roles.add("client");

        ServiceConfiguration config = new ServiceConfiguration();
        config.setAdvertisedAddress("localhost");
        config.setBrokerShutdownTimeoutMs(0L);
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        if (enableTls) {
            config.setWebServicePortTls(Optional.of(0));
        }
        config.setClientLibraryVersionCheckEnabled(enableFilter);
        config.setAuthenticationEnabled(enableAuth);
        config.setAuthenticationProviders(providers);
        config.setAuthorizationEnabled(false);
        config.setSuperUserRoles(roles);
        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        config.setTlsAllowInsecureConnection(allowInsecure);
        config.setTlsTrustCertsFilePath(allowInsecure ? "" : TLS_CLIENT_CERT_FILE_PATH);
        config.setClusterName("local");
        config.setAdvertisedAddress("localhost"); // TLS certificate expects localhost
        config.setZookeeperServers("localhost:2181");
        config.setHttpMaxRequestSize(10 * 1024);
        config.setDisableHttpDebugMethods(disableTrace);
        if (rateLimit > 0) {
            config.setHttpRequestsLimitEnabled(true);
            config.setHttpRequestsMaxPerSecond(rateLimit);
        }
        pulsar = spy(new PulsarService(config));
     // mock zk
        MockZooKeeper mockZooKeeper = MockedPulsarServiceBaseTest.createMockZooKeeper();
        ZooKeeperClientFactory mockZooKeeperClientFactory = new ZooKeeperClientFactory() {

             @Override
             public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType,
                     int zkSessionTimeoutMillis) {
                 // Always return the same instance (so that we don't loose the mock ZK content on broker restart
                 return CompletableFuture.completedFuture(mockZooKeeper);
             }
         };
        doReturn(mockZooKeeperClientFactory).when(pulsar).getZooKeeperClientFactory();
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(pulsar).createConfigurationMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(pulsar).createLocalMetadataStore();
        doReturn(new MockedBookKeeperClientFactory()).when(pulsar).newBookKeeperClientFactory();
        pulsar.start();

        try {
            pulsar.getLocalMetadataStore().delete("/minApiVersion", Optional.empty()).join();
        } catch (Exception ex) {
        }
        pulsar.getLocalMetadataStore().put("/minApiVersion", minApiVersion.getBytes(), Optional.of(-1L)).join();

        String BROKER_URL_BASE = "http://localhost:" + pulsar.getListenPortHTTP().get();
        String BROKER_URL_BASE_TLS = "https://localhost:" + pulsar.getListenPortHTTPS().orElse(-1);
        String serviceUrl = BROKER_URL_BASE;

        PulsarAdminBuilder adminBuilder = PulsarAdmin.builder();
        if (enableTls && enableAuth) {
            serviceUrl = BROKER_URL_BASE_TLS;

            Map<String, String> authParams = new HashMap<>();
            authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
            authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);

            adminBuilder.authentication(AuthenticationTls.class.getName(), authParams).allowTlsInsecureConnection(true);
        }

        BROKER_LOOKUP_URL = BROKER_URL_BASE
                + "/lookup/v2/destination/persistent/my-property/local/my-namespace/my-topic";
        BROKER_LOOKUP_URL_TLS = BROKER_URL_BASE_TLS
                + "/lookup/v2/destination/persistent/my-property/local/my-namespace/my-topic";

        PulsarAdmin pulsarAdmin = adminBuilder.serviceHttpUrl(serviceUrl).build();

        try {
            pulsarAdmin.clusters().createCluster(config.getClusterName(),
                    ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        } catch (ConflictException ce) {
            // This is OK.
        } finally {
            pulsarAdmin.close();
        }
    }

    @AfterMethod(alwaysRun = true)
    void teardown() {
        if (pulsar != null) {
            try {
                pulsar.close();
                pulsar = null;
            } catch (Exception e) {
                Assert.fail("Got exception while closing the pulsar instance ", e);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(WebServiceTest.class);
}
