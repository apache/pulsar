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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls.mapToString;
import static org.testng.AssertJUnit.fail;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.JacksonConfigurator;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.keystoretls.KeyStoreSSLContext;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-impl")
public class AdminApiKeyStoreTlsAuthTest extends ProducerConsumerBase {

    protected final String BROKER_KEYSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/broker.keystore.jks";
    protected final String BROKER_TRUSTSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/broker.truststore.jks";
    protected final String BROKER_KEYSTORE_PW = "111111";
    protected final String BROKER_TRUSTSTORE_PW = "111111";

    protected final String CLIENT_KEYSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/client.keystore.jks";
    protected final String CLIENT_TRUSTSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/client.truststore.jks";
    protected final String PROXY_KEYSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/proxy.keystore.jks";
    protected final String PROXY_AND_CLIENT_TRUSTSTORE_FILE_PATH =
            "./src/test/resources/authentication/keystoretls/proxy-and-client.truststore.jks";
    protected final String CLIENT_KEYSTORE_PW = "111111";
    protected final String CLIENT_TRUSTSTORE_PW = "111111";
    protected final String PROXY_KEYSTORE_PW = "111111";
    protected final String PROXY_AND_CLIENT_TRUSTSTORE_PW = "111111";

    protected final String CLIENT_KEYSTORE_CN = "clientuser";
    protected final String KEYSTORE_TYPE = "JKS";

    private final String clusterName = "test";
    Set<String> tlsProtocols = Sets.newConcurrentHashSet();

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setLoadBalancerEnabled(true);
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));

        conf.setTlsEnabledWithKeyStore(true);
        conf.setTlsKeyStoreType(KEYSTORE_TYPE);
        conf.setTlsKeyStore(BROKER_KEYSTORE_FILE_PATH);
        conf.setTlsKeyStorePassword(BROKER_KEYSTORE_PW);

        conf.setTlsTrustStoreType(KEYSTORE_TYPE);
        conf.setTlsTrustStore(PROXY_AND_CLIENT_TRUSTSTORE_FILE_PATH);
        conf.setTlsTrustStorePassword(PROXY_AND_CLIENT_TRUSTSTORE_PW);

        conf.setClusterName(clusterName);
        conf.setTlsRequireTrustedClientCertOnConnect(true);
        tlsProtocols.add("TLSv1.3");
        tlsProtocols.add("TLSv1.2");
        conf.setTlsProtocols(tlsProtocols);

        // config for authentication and authorization.
        conf.setSuperUserRoles(Sets.newHashSet(CLIENT_KEYSTORE_CN));
        conf.setProxyRoles(Sets.newHashSet("proxy"));
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        conf.setAuthenticationProviders(providers);

        conf.setBrokerClientTlsEnabled(true);
        conf.setBrokerClientTlsEnabledWithKeyStore(true);

        // set broker client tls auth
        Map<String, String> authParams = new HashMap<>();
        authParams.put(AuthenticationKeyStoreTls.KEYSTORE_TYPE, KEYSTORE_TYPE);
        authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PATH, CLIENT_KEYSTORE_FILE_PATH);
        authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PW, CLIENT_KEYSTORE_PW);
        conf.setBrokerClientAuthenticationPlugin(AuthenticationKeyStoreTls.class.getName());
        conf.setBrokerClientAuthenticationParameters(mapToString(authParams));
        conf.setBrokerClientTlsTrustStore(BROKER_TRUSTSTORE_FILE_PATH);
        conf.setBrokerClientTlsTrustStorePassword(BROKER_TRUSTSTORE_PW);
        conf.setNumExecutorThreadPoolSize(5);

        super.init();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    WebTarget buildWebClient() throws Exception {
        ClientConfig httpConfig = new ClientConfig();
        httpConfig.property(ClientProperties.FOLLOW_REDIRECTS, true);
        httpConfig.property(ClientProperties.ASYNC_THREADPOOL_SIZE, 8);
        httpConfig.register(MultiPartFeature.class);

        ClientBuilder clientBuilder = ClientBuilder.newBuilder().withConfig(httpConfig)
            .register(JacksonConfigurator.class).register(JacksonFeature.class);

        SSLContext sslCtx = KeyStoreSSLContext.createClientSslContext(
                KEYSTORE_TYPE,
                PROXY_KEYSTORE_FILE_PATH,
                PROXY_KEYSTORE_PW,
                KEYSTORE_TYPE,
                BROKER_TRUSTSTORE_FILE_PATH,
                BROKER_TRUSTSTORE_PW);

        clientBuilder.sslContext(sslCtx);
        Client client = clientBuilder.build();

        return client.target(brokerUrlTls.toString());
    }

    PulsarAdmin buildAdminClient() throws Exception {
        Map<String, String> authParams = new HashMap<>();
        authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PATH, CLIENT_KEYSTORE_FILE_PATH);
        authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PW, CLIENT_KEYSTORE_PW);

        return PulsarAdmin.builder()
                .serviceHttpUrl(brokerUrlTls.toString())
                .useKeyStoreTls(true)
                .tlsTrustStorePath(BROKER_TRUSTSTORE_FILE_PATH)
                .tlsTrustStorePassword(BROKER_TRUSTSTORE_PW)
                .allowTlsInsecureConnection(false)
                .authentication(AuthenticationKeyStoreTls.class.getName(), authParams)
                .build();
    }

    @Test
    public void testSuperUserCanListTenants() throws Exception {
        try (PulsarAdmin admin = buildAdminClient()) {
            admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
            admin.tenants().createTenant("tenant1",
                                         new TenantInfoImpl(ImmutableSet.of("foobar"),
                                                        ImmutableSet.of("test")));
            Assert.assertEquals(ImmutableSet.of("tenant1"), admin.tenants().getTenants());
        }
    }

    @Test
    public void testSuperUserCanListNamespaces() throws Exception {
        try (PulsarAdmin admin = buildAdminClient()) {
            admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
            admin.tenants().createTenant("tenant1",
                                         new TenantInfoImpl(ImmutableSet.of(""),
                                                        ImmutableSet.of("test")));
            admin.namespaces().createNamespace("tenant1/ns1");
            Assert.assertTrue(admin.namespaces().getNamespaces("tenant1").contains("tenant1/ns1"));
        }
    }

    @Test
    public void testAuthorizedUserAsOriginalPrincipal() throws Exception {
        try (PulsarAdmin admin = buildAdminClient()) {
            admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
            admin.tenants().createTenant("tenant1",
                                         new TenantInfoImpl(ImmutableSet.of("proxy", "user1"),
                                                        ImmutableSet.of("test")));
            admin.namespaces().createNamespace("tenant1/ns1");
        }
        WebTarget root = buildWebClient();
        Assert.assertEquals(ImmutableSet.of("tenant1/ns1"),
                            root.path("/admin/v2/namespaces").path("tenant1")
                            .request(MediaType.APPLICATION_JSON)
                            .header("X-Original-Principal", "user1")
                            .get(new GenericType<List<String>>() {}));
    }

    @Test
    public void testPersistentList() throws Exception {
        log.info("-- Starting {} test --", methodName);

        try (PulsarAdmin admin = buildAdminClient()) {
            admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
            admin.tenants().createTenant("tenant1",
                    new TenantInfoImpl(ImmutableSet.of("foobar"),
                            ImmutableSet.of("test")));
            Assert.assertEquals(ImmutableSet.of("tenant1"), admin.tenants().getTenants());

            admin.namespaces().createNamespace("tenant1/ns1");

            // this will calls internal admin to list nonpersist topics.
            admin.topics().getList("tenant1/ns1");
        } catch (PulsarAdminException ex) {
            ex.printStackTrace();
            fail("Should not have thrown an exception");
        }
    }
}
