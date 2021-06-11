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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Test(groups = "broker")
public class BrokerClientTlsAuthTest extends MockedPulsarServiceBaseTest {
    protected String methodName;

    public static final String CERT_TYPE = "pem";
    public static final String KEYSTORE_TYPE = "JKS";
    public static final String TRUSTSTORE_FILE_PATH = getTLSFile("ca.cert", KEYSTORE_TYPE);
    public static final String TRUSTSTORE_PW = "changeit";
    public static final String TLS_CLIENT_CERT_FILE_PATH = "./src/test/resources/authentication/tls/client-cert.pem";
    public static final String TLS_CLIENT_KEY_FILE_PATH = "./src/test/resources/authentication/tls/client-key.pem";


    @BeforeMethod
    public void beforeMethod(Method m) throws Exception {
        methodName = m.getName();
    }

    private static String getTLSFile(String name, String type) {
        return String.format("./src/test/resources/authentication/tls-http/%s." + type, name);
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
        conf.setTlsCertificateFilePath(getTLSFile("broker.cert", CERT_TYPE));
        conf.setTlsKeyFilePath(getTLSFile("broker.key-pk8", CERT_TYPE));
        conf.setAuthenticationEnabled(true);
        conf.setSuperUserRoles(ImmutableSet.of("superproxy", "broker.pulsar.apache.org"));
        conf.setAuthenticationProviders(
                ImmutableSet.of("org.apache.pulsar.broker.authentication.AuthenticationProviderTls"));
        conf.setAuthorizationEnabled(true);
        conf.setBrokerClientTlsEnabled(true);
        String str = String.format("tlsCertFile:%s,tlsKeyFile:%s", getTLSFile("broker.cert", CERT_TYPE),
                getTLSFile("broker.key-pk8", CERT_TYPE));
        conf.setBrokerClientAuthenticationParameters(str);
        conf.setBrokerClientAuthenticationPlugin("org.apache.pulsar.client.impl.auth.AuthenticationTls");
        conf.setBrokerClientTrustCertsFilePath(getTLSFile("ca.cert", CERT_TYPE));
        conf.setTlsAllowInsecureConnection(true);
        conf.setNumExecutorThreadPoolSize(5);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    PulsarAdmin buildAdminClient(String user) throws Exception {
        return PulsarAdmin.builder().allowTlsInsecureConnection(false).enableTlsHostnameVerification(false)
                .serviceHttpUrl(brokerUrlTls.toString())
                .authentication("org.apache.pulsar.client.impl.auth.AuthenticationTls",
                        String.format("tlsCertFile:%s,tlsKeyFile:%s", getTLSFile(user + ".cert", CERT_TYPE),
                                getTLSFile(user + ".key-pk8", CERT_TYPE)))
                .tlsTrustCertsFilePath(getTLSFile("ca.cert", CERT_TYPE)).build();
    }

    @Test
    public void testPEMCertWithTrustKeyStore() throws Exception {
        log.info("-- Starting {} test --", methodName);

        String tenant = "t1";
        String ns = tenant + "/test/ns1";
        String topicName = "persistent://" + ns + "/testPEMCertWithTrustKeyStore" + System.currentTimeMillis();

        try (PulsarAdmin admin = buildAdminClient("superproxy")) {
            ClusterDataImpl clusterData = new ClusterDataImpl();
            clusterData.setBrokerServiceUrl(brokerUrl.toString());
            admin.clusters().createCluster("test", clusterData);
            admin.tenants().createTenant(tenant, new TenantInfoImpl(ImmutableSet.of("admin"), ImmutableSet.of("test")));
            admin.namespaces().createNamespace(ns);
            admin.namespaces().grantPermissionOnNamespace(ns, "admin",
                    Sets.newHashSet(AuthAction.produce, AuthAction.consume));
        }

        Set<String> tlsProtocols = Sets.newConcurrentHashSet();
        tlsProtocols.add("TLSv1.2");

        // Use keystore for turst-store
        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrlTls()).enableTls(true)
                .useKeyStoreTls(true).allowTlsInsecureConnection(false).tlsTrustStorePath(TRUSTSTORE_FILE_PATH)
                .tlsTrustStorePassword(TRUSTSTORE_PW).tlsProtocols(tlsProtocols)
                .operationTimeout(1000, TimeUnit.MILLISECONDS);
        Map<String, String> authParams = new HashMap<>();
        // Use key-cert in pem format
        authParams.put("tlsCertFile", TLS_CLIENT_CERT_FILE_PATH);
        authParams.put("tlsKeyFile", TLS_CLIENT_KEY_FILE_PATH);
        clientBuilder.authentication(AuthenticationTls.class.getName(), authParams);
        @Cleanup
        PulsarClient pulsarClient = clientBuilder.build();

       Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscriptionType(SubscriptionType.Exclusive).subscribe();

        // consumer created successfully
        consumer.close();
    }
}
