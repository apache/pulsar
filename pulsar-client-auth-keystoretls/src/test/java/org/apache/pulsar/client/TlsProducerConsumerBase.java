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
package org.apache.pulsar.client;

import static org.mockito.Mockito.spy;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

// Base class for TLS authentication and authorization based on KeyStore type config.
public class TlsProducerConsumerBase extends ProducerConsumerBase {
    protected final String BROKER_KEYSTORE_FILE_PATH = "./src/test/resources/broker.keystore.jks";
    protected final String BROKER_TRUSTSTORE_FILE_PATH = "./src/test/resources/broker.truststore.jks";
    protected final String BROKER_KEYSTORE_PW = "111111";
    protected final String BROKER_TRUSTSTORE_PW = "111111";

    protected final String CLIENT_KEYSTORE_FILE_PATH = "./src/test/resources/client.keystore.jks";
    protected final String CLIENT_TRUSTSTORE_FILE_PATH = "./src/test/resources/client.truststore.jks";
    protected final String CLIENT_KEYSTORE_PW = "111111";
    protected final String CLIENT_TRUSTSTORE_PW = "111111";

    protected final String CLIENT_KEYSTORE_CN = "clientuser";
    protected final String KEYSTORE_TYPE = "JKS";

    private final String clusterName = "use";
    Set<String> tlsProtocols = Sets.newConcurrentHashSet();

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        // TLS configuration for Broker
        internalSetUpForBroker();

        // Start Broker
        super.init();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected void internalSetUpForBroker() throws Exception {
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsEnabledWithKeyStore(true);

        conf.setTlsKeyStoreType(KEYSTORE_TYPE);
        conf.setTlsKeyStore(BROKER_KEYSTORE_FILE_PATH);
        conf.setTlsKeyStorePassword(BROKER_KEYSTORE_PW);

        conf.setTlsTrustStoreType(KEYSTORE_TYPE);
        conf.setTlsTrustStore(CLIENT_TRUSTSTORE_FILE_PATH);
        conf.setTlsTrustStorePassword(CLIENT_TRUSTSTORE_PW);

        conf.setClusterName(clusterName);
        conf.setTlsRequireTrustedClientCertOnConnect(true);
        tlsProtocols.add("TLSv1.2");
        conf.setTlsProtocols(tlsProtocols);

        // config for authentication and authorization.
        conf.setSuperUserRoles(Sets.newHashSet(CLIENT_KEYSTORE_CN));
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderTls.class.getName());
        conf.setAuthenticationProviders(providers);
    }

    protected void internalSetUpForClient(boolean addCertificates, String lookupUrl) throws Exception {
        if (pulsarClient != null) {
            pulsarClient.close();
        }

        Set<String> tlsProtocols = Sets.newConcurrentHashSet();
        tlsProtocols.add("TLSv1.2");

        ClientBuilder clientBuilder = PulsarClient.builder().serviceUrl(lookupUrl)
                .enableTls(true)
                .useKeyStoreTls(true)
                .tlsTrustStorePath(BROKER_TRUSTSTORE_FILE_PATH)
                .tlsTrustStorePassword(BROKER_TRUSTSTORE_PW)
                .allowTlsInsecureConnection(false)
                .tlsProtocols(tlsProtocols)
                .operationTimeout(1000, TimeUnit.MILLISECONDS);
        if (addCertificates) {
            Map<String, String> authParams = new HashMap<>();
            authParams.put(AuthenticationKeyStoreTls.KEYSTORE_TYPE, KEYSTORE_TYPE);
            authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PATH, CLIENT_KEYSTORE_FILE_PATH);
            authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PW, CLIENT_KEYSTORE_PW);
            clientBuilder.authentication(AuthenticationKeyStoreTls.class.getName(), authParams);
        }
        pulsarClient = clientBuilder.build();
    }

    protected void internalSetUpForNamespace() throws Exception {
        Map<String, String> authParams = new HashMap<>();
        authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PATH, CLIENT_KEYSTORE_FILE_PATH);
        authParams.put(AuthenticationKeyStoreTls.KEYSTORE_PW, CLIENT_KEYSTORE_PW);

        if (admin != null) {
            admin.close();
        }

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrlTls.toString())
                .useKeyStoreTls(true)
                .tlsTrustStorePath(BROKER_TRUSTSTORE_FILE_PATH)
                .tlsTrustStorePassword(BROKER_TRUSTSTORE_PW)
                .allowTlsInsecureConnection(true)
                .authentication(AuthenticationKeyStoreTls.class.getName(), authParams).build());
        admin.clusters().createCluster(clusterName, new ClusterData(brokerUrl.toString(), brokerUrlTls.toString(),
                pulsar.getBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls()));
        admin.tenants().createTenant("my-property",
                new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/my-ns");
    }

}
