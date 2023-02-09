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
package org.apache.pulsar.broker.transaction;

import com.google.common.collect.Lists;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.SneakyThrows;
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
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.functions.utils.Exceptions;
import org.apache.pulsar.transaction.coordinator.TransactionCoordinatorID;
import org.apache.pulsar.transaction.coordinator.TxnMeta;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test for consuming transaction messages.
 */
@Slf4j
@Test(groups = "broker")
public class AuthenticatedTransactionProducerConsumerTest extends TransactionTestBase {

    private static final String TOPIC = NAMESPACE1 + "/txn-auth";

    private final String ADMIN_TOKEN;
    private final String TOKEN_PUBLIC_KEY;
    private final KeyPair kp;

    AuthenticatedTransactionProducerConsumerTest() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kp = kpg.generateKeyPair();

        byte[] encodedPublicKey = kp.getPublic().getEncoded();
        TOKEN_PUBLIC_KEY = "data:;base64," + Base64.getEncoder().encodeToString(encodedPublicKey);
        ADMIN_TOKEN = generateToken(kp, "admin");
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
        internalSetup();
        setUpBase(1, 1, TOPIC, 1);

        grantTxnLookupToRole("client");
        admin.namespaces().grantPermissionOnNamespace(NAMESPACE1, "client",
                EnumSet.allOf(AuthAction.class));
        grantTxnLookupToRole("client2");
    }

    @SneakyThrows
    private void grantTxnLookupToRole(String role) {
        admin.namespaces().grantPermissionOnNamespace(
                NamespaceName.SYSTEM_NAMESPACE.toString(),
                role,
                Sets.newHashSet(AuthAction.consume));
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

    @DataProvider(name = "actors")
    public Object[][] actors() {
        return new Object[][]{
                {"client", true},
                {"client", false},
                {"client2", true},
                {"client2", false},
                {"admin", true},
                {"admin", false}
        };
    }

    @Test(dataProvider = "actors")
    public void testEndTxn(String actor, boolean afterUnload) throws Exception {
        @Cleanup final PulsarClient pulsarClientOwner = PulsarClient.builder()
                .serviceUrl(pulsarServiceList.get(0).getBrokerServiceUrl())
                .authentication(AuthenticationFactory.token(generateToken(kp, "client")))
                .enableTransaction(true)
                .build();

        @Cleanup final PulsarClient pulsarClientOther = PulsarClient.builder()
                .serviceUrl(pulsarServiceList.get(0).getBrokerServiceUrl())
                .authentication(AuthenticationFactory.token(generateToken(kp, actor)))
                .enableTransaction(true)
                .build();
        Transaction transaction = pulsarClientOwner.newTransaction()
                .withTransactionTimeout(60, TimeUnit.SECONDS).build().get();

        @Cleanup final Consumer<String> consumer = pulsarClientOwner
                .newConsumer(Schema.STRING)
                .subscriptionName("test")
                .topic(TOPIC)
                .subscribe();


        @Cleanup final Producer<String> producer = pulsarClientOwner
                .newProducer(Schema.STRING)
                .sendTimeout(60, TimeUnit.SECONDS)
                .topic(TOPIC)
                .create();

        producer.newMessage().value("beforetxn").send();
        consumer.acknowledgeAsync(consumer.receive(5, TimeUnit.SECONDS).getMessageId(), transaction);
        producer.newMessage(transaction).value("message").send();
        if (afterUnload) {
            pulsarServiceList.get(0)
                    .getTransactionMetadataStoreService()
                    .removeTransactionMetadataStore(
                            TransactionCoordinatorID.get(transaction.getTxnID().getMostSigBits()));
        }

        final Throwable ex = syncGetException((
                (PulsarClientImpl) pulsarClientOther).getTcClient().commitAsync(transaction.getTxnID())
        );
        if (actor.equals("client") || actor.equals("admin")) {
            Assert.assertNull(ex);
            Assert.assertEquals(consumer.receive(5, TimeUnit.SECONDS).getValue(), "message");
        } else {
            Assert.assertNotNull(ex);
            Assert.assertTrue(ex instanceof TransactionCoordinatorClientException, ex.getClass().getName());
            Assert.assertNull(consumer.receive(5, TimeUnit.SECONDS));
            transaction.commit().get();
            Assert.assertEquals(consumer.receive(5, TimeUnit.SECONDS).getValue(), "message");
        }
    }

    @Test(dataProvider = "actors")
    public void testAddPartitionToTxn(String actor, boolean afterUnload) throws Exception {
        @Cleanup final PulsarClient pulsarClientOwner = PulsarClient.builder()
                .serviceUrl(pulsarServiceList.get(0).getBrokerServiceUrl())
                .authentication(AuthenticationFactory.token(generateToken(kp, "client")))
                .enableTransaction(true)
                .build();

        @Cleanup final PulsarClient pulsarClientOther = PulsarClient.builder()
                .serviceUrl(pulsarServiceList.get(0).getBrokerServiceUrl())
                .authentication(AuthenticationFactory.token(generateToken(kp, actor)))
                .enableTransaction(true)
                .build();
        Transaction transaction = pulsarClientOwner.newTransaction()
                .withTransactionTimeout(60, TimeUnit.SECONDS).build().get();

        if (afterUnload) {
            pulsarServiceList.get(0)
                    .getTransactionMetadataStoreService()
                    .removeTransactionMetadataStore(
                            TransactionCoordinatorID.get(transaction.getTxnID().getMostSigBits()));
        }

        final Throwable ex = syncGetException(((PulsarClientImpl) pulsarClientOther)
                .getTcClient().addPublishPartitionToTxnAsync(transaction.getTxnID(), Lists.newArrayList(TOPIC)));

        final TxnMeta txnMeta = pulsarServiceList.get(0).getTransactionMetadataStoreService()
                .getTxnMeta(transaction.getTxnID()).get();
        if (actor.equals("client") || actor.equals("admin")) {
            Assert.assertNull(ex);
            Assert.assertEquals(txnMeta.producedPartitions(), Lists.newArrayList(TOPIC));
        } else {
            Assert.assertNotNull(ex);
            Assert.assertTrue(ex instanceof TransactionCoordinatorClientException);
            Assert.assertTrue(txnMeta.producedPartitions().isEmpty());
        }
    }

    @Test(dataProvider = "actors")
    public void testAddSubscriptionToTxn(String actor, boolean afterUnload) throws Exception {
        @Cleanup final PulsarClient pulsarClientOwner = PulsarClient.builder()
                .serviceUrl(pulsarServiceList.get(0).getBrokerServiceUrl())
                .authentication(AuthenticationFactory.token(generateToken(kp, "client")))
                .enableTransaction(true)
                .build();

        @Cleanup final PulsarClient pulsarClientOther = PulsarClient.builder()
                .serviceUrl(pulsarServiceList.get(0).getBrokerServiceUrl())
                .authentication(AuthenticationFactory.token(generateToken(kp, actor)))
                .enableTransaction(true)
                .build();
        Transaction transaction = pulsarClientOwner.newTransaction()
                .withTransactionTimeout(60, TimeUnit.SECONDS).build().get();

        if (afterUnload) {
            pulsarServiceList.get(0)
                    .getTransactionMetadataStoreService()
                    .removeTransactionMetadataStore(
                            TransactionCoordinatorID.get(transaction.getTxnID().getMostSigBits()));
        }


        final Throwable ex = syncGetException(((PulsarClientImpl) pulsarClientOther)
                .getTcClient().addSubscriptionToTxnAsync(transaction.getTxnID(), TOPIC, "sub"));

        final TxnMeta txnMeta = pulsarServiceList.get(0).getTransactionMetadataStoreService()
                .getTxnMeta(transaction.getTxnID()).get();
        if (actor.equals("client") || actor.equals("admin")) {
            Assert.assertNull(ex);
            Assert.assertEquals(txnMeta.ackedPartitions().size(), 1);
        } else {
            Assert.assertNotNull(ex);
            Assert.assertTrue(ex instanceof TransactionCoordinatorClientException);
            Assert.assertTrue(txnMeta.ackedPartitions().isEmpty());
        }
    }

    @Test
    public void testNoAuth() throws Exception {
        try {
            @Cleanup final PulsarClient pulsarClient = PulsarClient.builder()
                    .serviceUrl(pulsarServiceList.get(0).getBrokerServiceUrl())
                    .enableTransaction(true)
                    .build();
            Assert.fail("should have failed");
        } catch (Exception t) {
            Assert.assertTrue(Exceptions.areExceptionsPresentInChain(t,
                    PulsarClientException.AuthenticationException.class));
        }
    }

    private static Throwable syncGetException(CompletableFuture<?> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            return e;
        } catch (ExecutionException e) {
            return FutureUtil.unwrapCompletionException(e);
        }
        return null;
    }
}
