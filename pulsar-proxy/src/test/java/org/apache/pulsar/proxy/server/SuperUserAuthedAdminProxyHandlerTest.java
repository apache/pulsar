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
package org.apache.pulsar.proxy.server;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;
import org.eclipse.jetty.servlet.ServletHolder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SuperUserAuthedAdminProxyHandlerTest extends MockedPulsarServiceBaseTest {
    private ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private WebServer webServer;
    private BrokerDiscoveryProvider discoveryProvider;
    private PulsarResources resource;

    static String getTlsFile(String name) {
        return String.format("./src/test/resources/authentication/tls-admin-proxy/%s.pem", name);
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        // enable tls and auth&auth at broker
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);

        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsTrustCertsFilePath(getTlsFile("ca.cert"));
        conf.setTlsCertificateFilePath(getTlsFile("broker.cert"));
        conf.setTlsKeyFilePath(getTlsFile("broker.key-pk8"));
        conf.setTlsAllowInsecureConnection(false);
        conf.setSuperUserRoles(ImmutableSet.of("admin", "superproxy"));
        conf.setProxyRoles(ImmutableSet.of("superproxy"));
        conf.setAuthenticationProviders(ImmutableSet.of(AuthenticationProviderTls.class.getName()));
        conf.setNumExecutorThreadPoolSize(5);

        super.internalSetup();

        // start proxy service
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(true);
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        proxyConfig.setTlsEnabledWithBroker(true);

        // enable tls and auth&auth at proxy
        proxyConfig.setTlsCertificateFilePath(getTlsFile("broker.cert"));
        proxyConfig.setTlsKeyFilePath(getTlsFile("broker.key-pk8"));
        proxyConfig.setTlsTrustCertsFilePath(getTlsFile("ca.cert"));

        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(
                String.format("tlsCertFile:%s,tlsKeyFile:%s",
                              getTlsFile("superproxy.cert"), getTlsFile("superproxy.key-pk8")));
        proxyConfig.setBrokerClientTrustCertsFilePath(getTlsFile("ca.cert"));
        proxyConfig.setAuthenticationProviders(ImmutableSet.of(AuthenticationProviderTls.class.getName()));

        resource = new PulsarResources(new ZKMetadataStore(mockZooKeeper),
                new ZKMetadataStore(mockZooKeeperGlobal));
        webServer = new WebServer(proxyConfig, new AuthenticationService(
                                          PulsarConfigurationLoader.convertFrom(proxyConfig)));
        discoveryProvider = spy(new BrokerDiscoveryProvider(proxyConfig, resource));
        LoadManagerReport report = new LoadReport(brokerUrl.toString(), brokerUrlTls.toString(), null, null);
        doReturn(report).when(discoveryProvider).nextBroker();

        ServletHolder servletHolder = new ServletHolder(new AdminProxyHandler(proxyConfig, discoveryProvider));
        webServer.addServlet("/admin", servletHolder);
        webServer.addServlet("/lookup", servletHolder);

        // start web-service
        webServer.start();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        webServer.stop();
        super.internalCleanup();
    }

    PulsarAdmin getAdminClient(String user) throws Exception {
        return PulsarAdmin.builder()
            .serviceHttpUrl("https://localhost:" + webServer.getListenPortHTTPS().get())
            .tlsTrustCertsFilePath(getTlsFile("ca.cert"))
            .allowTlsInsecureConnection(false)
            .authentication(AuthenticationTls.class.getName(),
                    ImmutableMap.of("tlsCertFile", getTlsFile(user + ".cert"),
                                    "tlsKeyFile", getTlsFile(user + ".key-pk8")))
            .build();
    }

    @Test
    public void testAuthenticatedProxyAsAdmin() throws Exception {
        try (PulsarAdmin adminAdmin = getAdminClient("admin")) {
            adminAdmin.clusters().createCluster(configClusterName, ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
            adminAdmin.tenants().createTenant("tenant1",
                                              new TenantInfoImpl(ImmutableSet.of("randoUser"),
                                                             ImmutableSet.of(configClusterName)));
            Assert.assertEquals(ImmutableSet.of("tenant1"), adminAdmin.tenants().getTenants());
        }
    }

    @Test
    public void testAuthenticatedProxyAsNonAdmin() throws Exception {
        try (PulsarAdmin adminAdmin = getAdminClient("admin");
             PulsarAdmin user1Admin = getAdminClient("user1")) {
            try {
                user1Admin.tenants().getTenants();
                Assert.fail("Shouldn't be able to do superuser operation");
            } catch (PulsarAdminException.NotAuthorizedException e) {
                // expected
            }
            adminAdmin.clusters().createCluster(configClusterName, ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
            adminAdmin.tenants().createTenant("tenant1",
                                              new TenantInfoImpl(ImmutableSet.of("unknownUser"),
                                                             ImmutableSet.of(configClusterName)));
            adminAdmin.namespaces().createNamespace("tenant1/ns1");
            Assert.assertEquals(ImmutableSet.of("tenant1/ns1"), adminAdmin.namespaces().getNamespaces("tenant1"));
            try {
                user1Admin.namespaces().getNamespaces("tenant1");
                Assert.fail("Shouldn't have access to namespace yet");
            } catch (PulsarAdminException.NotAuthorizedException e) {
                // expected
            }
            adminAdmin.tenants().updateTenant("tenant1",
                                              new TenantInfoImpl(ImmutableSet.of("user1"),
                                                             ImmutableSet.of(configClusterName)));
            Assert.assertEquals(ImmutableSet.of("tenant1/ns1"), user1Admin.namespaces().getNamespaces("tenant1"));
        }
    }

    @Test
    public void testAuthWithRandoCert() throws Exception {
        // test that we cannot connect or do anything with a cert not signed by CA
        try (PulsarAdmin randoAdmin = getAdminClient("randouser")) {
            try {
                randoAdmin.tenants().getTenants();
                Assert.fail("Shouldn't be able to do anything");
            } catch (PulsarAdminException.NotAuthorizedException e) {
                // expected
            }
        }
    }
}
