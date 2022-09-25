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

package org.apache.pulsar.client.api;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import javax.crypto.SecretKey;
import java.time.Duration;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Test(groups = "broker-api")
@Slf4j
public class TokenExpirationProduceConsumerTest extends TlsProducerConsumerBase {
    private final String tenant ="my-tenant";
    private final NamespaceName namespaceName = NamespaceName.get("my-tenant","my-ns");

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        // TLS configuration for Broker
        internalSetUpForBroker();

        // Start Broker
        super.init();

        admin = getAdmin(ADMIN_TOKEN);
        admin.clusters().createCluster(configClusterName,
                ClusterData.builder()
                        .serviceUrl(brokerUrl.toString())
                        .serviceUrlTls(brokerUrlTls.toString())
                        .brokerServiceUrl(pulsar.getBrokerServiceUrl())
                        .brokerServiceUrlTls(pulsar.getBrokerServiceUrlTls())
                        .build());
        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet(configClusterName)));
        admin.namespaces().createNamespace(namespaceName.toString());
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    private static final SecretKey SECRET_KEY = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);
    public static final String ADMIN_TOKEN = Jwts.builder().setSubject("admin").signWith(SECRET_KEY).compact();

    public String getExpireToken(String role, Date date) {
        return Jwts.builder().setSubject(role).signWith(SECRET_KEY)
                .setExpiration(date).compact();
    }

    protected void internalSetUpForBroker() {
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        conf.setTlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH);
        conf.setClusterName(configClusterName);
        conf.setAuthenticationRefreshCheckSeconds(1);
        conf.setTlsRequireTrustedClientCertOnConnect(false);
        conf.setTlsAllowInsecureConnection(false);
        conf.setAuthenticationEnabled(true);
        conf.setTransactionCoordinatorEnabled(true);
        conf.setSuperUserRoles(Sets.newHashSet("admin"));
        conf.setAuthenticationProviders(Sets.newHashSet(AuthenticationProviderToken.class.getName()));
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters("token:" + ADMIN_TOKEN);
        conf.getProperties().setProperty("tokenSecretKey", "data:;base64,"
                + Base64.getEncoder().encodeToString(SECRET_KEY.getEncoded()));
    }

    private PulsarClient getClient(String token) throws Exception {
        ClientBuilder clientBuilder = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrlTls())
                .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH)
                .enableTls(true)
                .allowTlsInsecureConnection(false)
                .enableTlsHostnameVerification(true)
                .authentication(AuthenticationToken.class.getName(),"token:" +token)
                .operationTimeout(1000, TimeUnit.MILLISECONDS);
        return clientBuilder.build();
    }

    private PulsarAdmin getAdmin(String token) throws Exception {
        PulsarAdminBuilder clientBuilder = PulsarAdmin.builder().serviceHttpUrl(pulsar.getWebServiceAddressTls())
                .tlsTrustCertsFilePath(TLS_TRUST_CERT_FILE_PATH)
                .allowTlsInsecureConnection(false)
                .authentication(AuthenticationToken.class.getName(),"token:" +token)
                .enableTlsHostnameVerification(true);
        return clientBuilder.build();
    }

    @Test
    public void testTokenExpirationProduceConsumer() throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.SECOND, 20);
        String role = "test";
        String token = getExpireToken(role, calendar.getTime());
        Date expiredTime = calendar.getTime();

        Set<AuthAction> permissions = new HashSet<>();
        permissions.add(AuthAction.consume);
        permissions.add(AuthAction.produce);
        admin.namespaces().grantPermissionOnNamespace(namespaceName.toString(), role, permissions);

        @Cleanup
        PulsarClient pulsarClient = getClient(token);
        String topic = namespaceName + "/test-token";

        @Cleanup final Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("test-token")
                .subscribe();
        @Cleanup final Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        Awaitility.await().timeout(Duration.ofSeconds(60)).pollInterval(3, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThrows(PulsarClientException.TimeoutException.class, () -> {
                producer.send("heart beat".getBytes());
                Message<byte[]> message = consumer.receive();
                consumer.acknowledge(message);
            });
            assertTrue(new Date().compareTo(expiredTime) > 0);
        });
    }
}
