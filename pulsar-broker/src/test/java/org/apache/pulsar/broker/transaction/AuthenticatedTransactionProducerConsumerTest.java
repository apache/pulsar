/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.transaction;

import com.google.common.collect.Sets;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.time.Duration;
import java.util.Base64;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.authentication.AuthenticationProviderToken;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test for consuming transaction messages.
 */
@Slf4j
@Test(groups = "broker")
public class AuthenticatedTransactionProducerConsumerTest extends TransactionTestBase {

    private static final String CONSUME_TOPIC = "persistent://public/txn/txn-consume-test";

    private final String ADMIN_TOKEN;
    private final String CLIENT_TOKEN;
    private final String TOKEN_PUBLIC_KEY;

    AuthenticatedTransactionProducerConsumerTest() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        KeyPair kp = kpg.generateKeyPair();

        byte[] encodedPublicKey = kp.getPublic().getEncoded();
        TOKEN_PUBLIC_KEY = "data:;base64," + Base64.getEncoder().encodeToString(encodedPublicKey);
        ADMIN_TOKEN = generateToken(kp, "admin");
        CLIENT_TOKEN = generateToken(kp, "client");
    }


    private String generateToken(KeyPair kp, String subject) {
        PrivateKey pkey = kp.getPrivate();
        long expMillis = System.currentTimeMillis() + Duration.ofHours(1).toMillis();
        Date exp = new Date(expMillis);

        return Jwts.builder()
                .setSubject(subject)
                .setExpiration(exp)
                .signWith(pkey, SignatureAlgorithm.forSigningKey(pkey))
                .compact();
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);

        Set<String> superUserRoles = new HashSet<>();
        superUserRoles.add("admin");
        conf.setSuperUserRoles(superUserRoles);

        Set<String> providers = new HashSet<>();
        providers.add(AuthenticationProviderToken.class.getName());
        conf.setAuthenticationProviders(providers);

        // Set provider domain name
        Properties properties = new Properties();
        properties.setProperty("tokenPublicKey", TOKEN_PUBLIC_KEY);

        conf.setProperties(properties);
        conf.setBrokerClientAuthenticationPlugin(AuthenticationToken.class.getName());
        conf.setBrokerClientAuthenticationParameters("token:" + ADMIN_TOKEN);
        setBrokerCount(1);
        super.internalSetupAndInitCluster();


        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet(), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace("public/txn", 10);
        admin.topics().createNonPartitionedTopic(CONSUME_TOPIC);

        admin.namespaces().grantPermissionOnNamespace("public/txn", "client",
                EnumSet.allOf(AuthAction.class));
    }

    @Override
    protected PulsarClient createNewPulsarClient(ClientBuilder clientBuilder) throws PulsarClientException {
        return clientBuilder
                .enableTransaction(true)
                .authentication(AuthenticationFactory.token(ADMIN_TOKEN))
                .build();
    }

    @Override
    protected PulsarAdmin createNewPulsarAdmin(PulsarAdminBuilder builder) throws PulsarClientException {
        return builder
                .authentication(AuthenticationFactory.token(ADMIN_TOKEN))
                .build();
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void testProduceAndConsume() throws Exception {
        @Cleanup final PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarServiceList.get(0).getBrokerServiceUrl())
                .authentication(AuthenticationFactory.token(CLIENT_TOKEN))
                .enableTransaction(true)
                .build();

        Transaction transaction = pulsarClient.newTransaction()
                .withTransactionTimeout(60, TimeUnit.SECONDS).build().get();

        @Cleanup final Consumer<String> consumer = pulsarClient
                .newConsumer(Schema.STRING)
                .subscriptionName("test")
                .topic(CONSUME_TOPIC)
                .subscribe();


        @Cleanup final Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .sendTimeout(60, TimeUnit.SECONDS)
                .topic(CONSUME_TOPIC)
                .create();

        producer.newMessage().value("message").send();
        consumer.acknowledgeAsync(consumer.receive(5, TimeUnit.SECONDS).getMessageId(), transaction)
                .get(5, TimeUnit.SECONDS);

        producer.newMessage(transaction).value("message2").send();
        transaction.commit();
        Assert.assertEquals(consumer.receive(5, TimeUnit.SECONDS).getValue(), "message2");
    }
}
