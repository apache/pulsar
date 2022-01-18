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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.fail;

import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Method;
import java.util.Optional;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class BrokerAdminClientTlsAuthTest extends MockedPulsarServiceBaseTest {
    protected String methodName;

    @BeforeMethod
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
    }

    private static String getTLSFile(String name) {
        return String.format("./src/test/resources/authentication/tls-http/%s.pem", name);
    }

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        buildConf(conf);
        super.internalSetup();
    }

    private void buildConf(ServiceConfiguration conf) {
        conf.setLoadBalancerEnabled(true);
        conf.setTlsCertificateFilePath(getTLSFile("broker.cert"));
        conf.setTlsKeyFilePath(getTLSFile("broker.key-pk8"));
        conf.setTlsTrustCertsFilePath(getTLSFile("ca.cert"));
        conf.setAuthenticationEnabled(true);
        conf.setSuperUserRoles(ImmutableSet.of("superproxy", "broker.pulsar.apache.org"));
        conf.setAuthenticationProviders(
                ImmutableSet.of("org.apache.pulsar.broker.authentication.AuthenticationProviderTls"));
        conf.setAuthorizationEnabled(true);
        conf.setBrokerClientTlsEnabled(true);
        String str = String.format("tlsCertFile:%s,tlsKeyFile:%s", getTLSFile("broker.cert"), getTLSFile("broker.key-pk8"));
        conf.setBrokerClientAuthenticationParameters(str);
        conf.setBrokerClientAuthenticationPlugin("org.apache.pulsar.client.impl.auth.AuthenticationTls");
        conf.setBrokerClientTrustCertsFilePath(getTLSFile("ca.cert"));
        conf.setTlsAllowInsecureConnection(true);
        conf.setNumExecutorThreadPoolSize(5);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    PulsarAdmin buildAdminClient(String user) throws Exception {
        return PulsarAdmin.builder()
            .allowTlsInsecureConnection(false)
            .enableTlsHostnameVerification(false)
            .serviceHttpUrl(brokerUrlTls.toString())
            .authentication("org.apache.pulsar.client.impl.auth.AuthenticationTls",
                            String.format("tlsCertFile:%s,tlsKeyFile:%s",
                                          getTLSFile(user + ".cert"), getTLSFile(user + ".key-pk8")))
            .tlsTrustCertsFilePath(getTLSFile("ca.cert")).build();
    }

    /**
     * Test case => Use Multiple Brokers
     *           => Create a namespace with bundles distributed among these brokers.
     *           => Use Tls as authPlugin for everything.
     *           => Run list topics command
     * @throws Exception
     */
    @Test
    public void testPersistentList() throws Exception {
        log.info("-- Starting {} test --", methodName);

        /***** Start Broker 2 ******/
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setBrokerShutdownTimeoutMs(0L);
        conf.setBrokerServicePort(Optional.of(0));
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePort(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setAdvertisedAddress("localhost");
        conf.setClusterName(this.conf.getClusterName());
        conf.setZookeeperServers("localhost:2181");
        conf.setConfigurationStoreServers("localhost:3181");
        buildConf(conf);

        @Cleanup
        PulsarService pulsar2 = startBroker(conf);

        /***** Broker 2 Started *****/
        try (PulsarAdmin admin = buildAdminClient("superproxy")) {
            admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
            admin.tenants().createTenant("tenant",
                                         new TenantInfoImpl(ImmutableSet.of("admin"),
                                                        ImmutableSet.of("test")));
        }
        try (PulsarAdmin admin = buildAdminClient("admin")) {
            Policies policies = new Policies();
            policies.bundles = BundlesData.builder().numBundles(4).build();
            policies.replication_clusters = ImmutableSet.of("test");
            admin.namespaces().createNamespace("tenant/ns", policies);
            try {
                admin.topics().getList("tenant/ns");
            } catch (PulsarAdminException ex) {
                ex.printStackTrace();
                fail("Should not have thrown an exception");
            }
            String topicName = String.format("persistent://%s/t1", "tenant/ns");
            admin.lookups().lookupTopic(topicName);
        }

    }
}
