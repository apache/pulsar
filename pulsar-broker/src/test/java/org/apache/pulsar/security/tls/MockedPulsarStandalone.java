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
package org.apache.pulsar.security.tls;

import static org.apache.pulsar.utils.ResourceUtils.getAbsolutePath;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;


public abstract class MockedPulsarStandalone implements AutoCloseable {

    @Getter
    private final ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
    private PulsarTestContext pulsarTestContext;

    @Getter
    private PulsarService pulsarService;
    private PulsarAdmin serviceInternalAdmin;


    {
        serviceConfiguration.setClusterName(TEST_CLUSTER_NAME);
        serviceConfiguration.setBrokerShutdownTimeoutMs(0L);
        serviceConfiguration.setBrokerServicePort(Optional.of(0));
        serviceConfiguration.setBrokerServicePortTls(Optional.of(0));
        serviceConfiguration.setAdvertisedAddress("localhost");
        serviceConfiguration.setWebServicePort(Optional.of(0));
        serviceConfiguration.setWebServicePortTls(Optional.of(0));
        serviceConfiguration.setNumExecutorThreadPoolSize(5);
        serviceConfiguration.setExposeBundlesMetricsInPrometheus(true);
    }

    @SneakyThrows
    protected void loadECTlsCertificateWithFile() {
        serviceConfiguration.setTlsEnabled(true);
        serviceConfiguration.setBrokerServicePort(Optional.empty());
        serviceConfiguration.setWebServicePort(Optional.empty());
        serviceConfiguration.setTlsTrustCertsFilePath(TLS_EC_TRUSTED_CERT_PATH);
        serviceConfiguration.setTlsCertificateFilePath(TLS_EC_SERVER_CERT_PATH);
        serviceConfiguration.setTlsKeyFilePath(TLS_EC_SERVER_KEY_PATH);
        serviceConfiguration.setBrokerClientTlsEnabled(true);
        serviceConfiguration.setBrokerClientTrustCertsFilePath(TLS_EC_TRUSTED_CERT_PATH);
        serviceConfiguration.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        final Map<String, String> brokerClientAuthParams = new HashMap<>();
        brokerClientAuthParams.put("tlsCertFile", TLS_EC_BROKER_CLIENT_CERT_PATH);
        brokerClientAuthParams.put("tlsKeyFile", TLS_EC_BROKER_CLIENT_KEY_PATH);
        serviceConfiguration.setBrokerClientAuthenticationParameters(mapper.writeValueAsString(brokerClientAuthParams));
    }

    @SneakyThrows
    protected void loadECTlsCertificateWithKeyStore() {
        serviceConfiguration.setTlsEnabled(true);
        serviceConfiguration.setBrokerServicePort(Optional.empty());
        serviceConfiguration.setWebServicePort(Optional.empty());
        serviceConfiguration.setTlsEnabledWithKeyStore(true);
        serviceConfiguration.setTlsKeyStore(TLS_EC_KS_SERVER_STORE);
        serviceConfiguration.setTlsKeyStorePassword(TLS_EC_KS_SERVER_PASS);
        serviceConfiguration.setTlsTrustStore(TLS_EC_KS_TRUSTED_STORE);
        serviceConfiguration.setTlsTrustStorePassword(TLS_EC_KS_TRUSTED_STORE_PASS);
        serviceConfiguration.setBrokerClientTlsEnabled(true);
        serviceConfiguration.setBrokerClientTlsEnabledWithKeyStore(true);
        serviceConfiguration.setBrokerClientTlsKeyStore(TLS_EC_KS_BROKER_CLIENT_STORE);
        serviceConfiguration.setBrokerClientTlsKeyStorePassword(TLS_EC_KS_BROKER_CLIENT_PASS);
        serviceConfiguration.setBrokerClientTlsTrustStore(TLS_EC_KS_TRUSTED_STORE);
        serviceConfiguration.setBrokerClientTlsTrustStorePassword(TLS_EC_KS_TRUSTED_STORE_PASS);
        serviceConfiguration.setBrokerClientAuthenticationPlugin(AuthenticationKeyStoreTls.class.getName());
        final Map<String, String> brokerClientAuthParams = new HashMap<>();
        brokerClientAuthParams.put("keyStorePath", TLS_EC_KS_BROKER_CLIENT_STORE);
        brokerClientAuthParams.put("keyStorePassword", TLS_EC_KS_BROKER_CLIENT_PASS);
        serviceConfiguration.setBrokerClientAuthenticationParameters(mapper.writeValueAsString(brokerClientAuthParams));
    }

    protected void enableTlsAuthentication() {
        serviceConfiguration.setAuthenticationEnabled(true);
        serviceConfiguration.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderTls.class.getName()));
    }

    @SneakyThrows
    protected void start() {
        this.pulsarTestContext = PulsarTestContext.builder()
                .spyByDefault()
                .config(serviceConfiguration)
                .withMockZookeeper(false)
                .build();
        this.pulsarService = pulsarTestContext.getPulsarService();
        this.serviceInternalAdmin = pulsarService.getAdminClient();
        setupDefaultTenantAndNamespace();
    }

    private void setupDefaultTenantAndNamespace() throws Exception {
        if (!serviceInternalAdmin.clusters().getClusters().contains(TEST_CLUSTER_NAME)) {
            serviceInternalAdmin.clusters().createCluster(TEST_CLUSTER_NAME,
                    ClusterData.builder().serviceUrl(pulsarService.getWebServiceAddress()).build());
        }
        if (!serviceInternalAdmin.tenants().getTenants().contains(DEFAULT_TENANT)) {
            serviceInternalAdmin.tenants().createTenant(DEFAULT_TENANT, TenantInfo.builder().allowedClusters(
                    Sets.newHashSet(TEST_CLUSTER_NAME)).build());
        }
        if (!serviceInternalAdmin.namespaces().getNamespaces(DEFAULT_TENANT).contains(DEFAULT_NAMESPACE)) {
            serviceInternalAdmin.namespaces().createNamespace(DEFAULT_NAMESPACE);
        }
    }


    @Override
    public void close() throws Exception {
        if (pulsarTestContext != null) {
            pulsarTestContext.close();
        }
    }

    // Utils
    protected static final ObjectMapper mapper = new ObjectMapper();

    // Static name
    private static final String DEFAULT_TENANT = "public";
    private static final String DEFAULT_NAMESPACE = "public/default";
    private static final String TEST_CLUSTER_NAME = "test-standalone";

    // EC certificate
    protected static final String TLS_EC_TRUSTED_CERT_PATH =
            getAbsolutePath("certificate-authority/ec/ca.cert.pem");
    private static final String TLS_EC_SERVER_KEY_PATH =
            getAbsolutePath("certificate-authority/ec/server.key-pk8.pem");
    private static final String TLS_EC_SERVER_CERT_PATH =
            getAbsolutePath("certificate-authority/ec/server.cert.pem");
    private static final String TLS_EC_BROKER_CLIENT_KEY_PATH =
            getAbsolutePath("certificate-authority/ec/broker_client.key-pk8.pem");
    private static final String TLS_EC_BROKER_CLIENT_CERT_PATH =
            getAbsolutePath("certificate-authority/ec/broker_client.cert.pem");
    protected static final String TLS_EC_CLIENT_KEY_PATH =
            getAbsolutePath("certificate-authority/ec/client.key-pk8.pem");
    protected static final String TLS_EC_CLIENT_CERT_PATH =
            getAbsolutePath("certificate-authority/ec/client.cert.pem");

    // EC KeyStore
    private static final String TLS_EC_KS_SERVER_STORE =
            getAbsolutePath("certificate-authority/ec/jks/server.keystore.jks");
    private static final String TLS_EC_KS_SERVER_PASS = "serverpw";
    private static final String TLS_EC_KS_BROKER_CLIENT_STORE =
            getAbsolutePath("certificate-authority/ec/jks/broker_client.keystore.jks");
    private static final String TLS_EC_KS_BROKER_CLIENT_PASS = "brokerclientpw";
    protected static final String TLS_EC_KS_CLIENT_STORE =
            getAbsolutePath("certificate-authority/ec/jks/client.keystore.jks");
    protected static final String TLS_EC_KS_CLIENT_PASS = "clientpw";
    protected static final String TLS_EC_KS_TRUSTED_STORE =
            getAbsolutePath("certificate-authority/ec/jks/ca.truststore.jks");
    protected static final String TLS_EC_KS_TRUSTED_STORE_PASS = "rootpw";
}
