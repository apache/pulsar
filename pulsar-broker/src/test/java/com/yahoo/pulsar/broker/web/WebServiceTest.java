/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.web;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.apache.bookkeeper.test.PortManager;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.admin.PulsarAdminException.ConflictException;
import com.yahoo.pulsar.client.api.Authentication;
import com.yahoo.pulsar.client.api.ClientConfiguration;
import com.yahoo.pulsar.client.impl.auth.AuthenticationTls;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.common.util.SecurityUtility;
import com.yahoo.pulsar.zookeeper.MockedZooKeeperClientFactoryImpl;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

/**
 * Tests for the {@code WebService} class. Note that this test only covers the newly added ApiVersionFilter related
 * tests for now as this test class was added quite a bit after the class was written.
 *
 */
public class WebServiceTest {

    private PulsarService pulsar;
    private final static int BROKER_WEBSERVICE_PORT = PortManager.nextFreePort();
    private final static int BROKER_WEBSERVICE_PORT_TLS = PortManager.nextFreePort();
    private static final String BROKER_URL_BASE = "http://localhost:" + BROKER_WEBSERVICE_PORT;
    private static final String BROKER_URL_BASE_TLS = "https://localhost:" + BROKER_WEBSERVICE_PORT_TLS;
    private static final String BROKER_LOOKUP_URL = BROKER_URL_BASE
            + "/lookup/v2/destination/persistent/my-property/local/my-namespace/my-topic";
    private static final String BROKER_LOOKUP_URL_TLS = BROKER_URL_BASE_TLS
            + "/lookup/v2/destination/persistent/my-property/local/my-namespace/my-topic";
    private static final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
    private static final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";
    private static final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/certificate/client.crt";
    private static final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/certificate/client.key";

    /**
     * Test that if the enableClientVersionCheck option is enabled, the {@code ApiVersionFilter} is added to the filter
     * chain. We test this indirectly by creating live PulsarService and making an http call to it.
     */
    @Test
    public void testFilterEnabled() throws Exception {
        setupEnv(true, "1.0", false, false, false, false);

        // Make an HTTP request to lookup a namespace. The request should fail
        // with a 400 error.
        try {
            makeHttpRequest(false, false);
            Assert.fail("Request should have failed."); // We should have gotten an exception on the previous
            // line.
        } catch (IOException ex) {
            Assert.assertTrue(ex.getMessage().contains("HTTP response code: 400"));
        }
    }

    /**
     * Test that if the enableClientVersionCheck option is disabled, the {@code ApiVersionFilter} is not added to the
     * filter chain. We test this indirectly by creating live PulsarService and making an http call to it.
     *
     */
    @Test
    public void testFilterDisabled() throws Exception {
        setupEnv(false, "1.0", false, false, false, false);

        try {
            // Make an HTTP request to lookup a namespace. The request should
            // succeed
            makeHttpRequest(false, false);
        } catch (Exception e) {
            Assert.fail("HTTP request to lookup a namespace shouldn't fail ", e);
        }
    }

    /**
     * Test that the {@WebService} class properly passes the allowUnversionedClients value. We do this by setting
     * allowUnversionedClients to true, then making a request with no version, which should go through.
     *
     */
    @Test
    public void testDefaultClientVersion() throws Exception {
        setupEnv(true, "1.0", true, false, false, false);

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
        setupEnv(false, "1.0", false, true, false, false);

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
        setupEnv(false, "1.0", false, false, false, false);

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
            Assert.assertTrue(e.getMessage().contains("Connection refused"));
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
        setupEnv(false, "1.0", false, true, true, true);

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
        setupEnv(false, "1.0", false, true, true, false);

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
    public void testSplitPath() {
        String result = PulsarWebResource.splitPath("prop/cluster/ns/topic1", 4);
        Assert.assertEquals(result, "topic1");
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
            boolean enableTls, boolean enableAuth, boolean allowInsecure) throws Exception {
        Set<String> providers = new HashSet<>();
        providers.add("com.yahoo.pulsar.broker.authentication.AuthenticationProviderTls");

        Set<String> roles = new HashSet<>();
        roles.add("client");

        ServiceConfiguration config = new ServiceConfiguration();
        config.setWebServicePort(BROKER_WEBSERVICE_PORT);
        config.setWebServicePortTls(BROKER_WEBSERVICE_PORT_TLS);
        config.setClientLibraryVersionCheckEnabled(enableFilter);
        config.setAuthenticationEnabled(enableAuth);
        config.setAuthenticationProviders(providers);
        config.setAuthorizationEnabled(false);
        config.setClientLibraryVersionCheckAllowUnversioned(allowUnversionedClients);
        config.setSuperUserRoles(roles);
        config.setTlsEnabled(enableTls);
        config.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        config.setTlsAllowInsecureConnection(allowInsecure);
        config.setTlsTrustCertsFilePath(allowInsecure ? "" : TLS_CLIENT_CERT_FILE_PATH);
        config.setClusterName("local");
        config.setAdvertisedAddress("localhost"); // TLS certificate expects localhost
        pulsar = spy(new PulsarService(config));
        doReturn(new MockedZooKeeperClientFactoryImpl()).when(pulsar).getZooKeeperClientFactory();
        pulsar.start();

        try {
            pulsar.getZkClient().delete("/minApiVersion", -1);
        } catch (Exception ex) {
        }
        pulsar.getZkClient().create("/minApiVersion", minApiVersion.getBytes(), null, CreateMode.PERSISTENT);

        String serviceUrl = BROKER_URL_BASE;
        ClientConfiguration clientConfig = new ClientConfiguration();

        if (enableTls && enableAuth) {
            serviceUrl = BROKER_URL_BASE_TLS;

            Map<String, String> authParams = new HashMap<>();
            authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
            authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
            Authentication auth = new AuthenticationTls();
            auth.configure(authParams);

            clientConfig.setAuthentication(auth);
            clientConfig.setUseTls(true);
            clientConfig.setTlsAllowInsecureConnection(true);
        }

        PulsarAdmin pulsarAdmin = new PulsarAdmin(new URL(serviceUrl), clientConfig);

        try {
            pulsarAdmin.clusters().createCluster(config.getClusterName(),
                    new ClusterData(pulsar.getWebServiceAddress()));
        } catch (ConflictException ce) {
            // This is OK.
        } finally {
            pulsarAdmin.close();
        }
    }

    @AfterMethod(alwaysRun = true)
    void teardown() throws Exception {
        try {
            pulsar.close();
        } catch (Exception e) {
            Assert.fail("Got exception while closing the pulsar instance ", e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(WebServiceTest.class);
}
